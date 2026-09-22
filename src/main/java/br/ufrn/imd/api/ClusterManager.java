package br.ufrn.imd.api;

import br.ufrn.imd.clusters.ServiceInstance;
import br.ufrn.imd.enums.NodeRole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterManager {

    /** Registra instâncias ativas por Serviço: "RESERVATION" -> {@code Map<instanceId, ServiceInstance>} */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ServiceInstance>> registry = new ConcurrentHashMap<>();

    /** Contadores para Load Balancing Round-Robin simples entre followers/read-replicas se necessário */
    private final ConcurrentHashMap<String, AtomicInteger> roundRobinIndexes = new ConcurrentHashMap<>();

    // Guarda o maior TERMO conhecido por serviço para evitar "Líderes do Passado"
    private final ConcurrentHashMap<String, Long> serviceTerms = new ConcurrentHashMap<>();

    private final ScheduledExecutorService healthChecker = Executors.newSingleThreadScheduledExecutor();
    private static final long HEARTBEAT_TIMEOUT_MS = 5000; // 5 segundos sem Heartbeat = Instância Morta

    public ClusterManager() {
        // Tarefa em background que varre e remove instâncias mortas a cada 1.5 segundos
        healthChecker.scheduleAtFixedRate(this::purgeDeadInstances, 1500, 1500, TimeUnit.MILLISECONDS);
    }


    /**
     * Processa o Heartbeat enviado pelas instâncias (primeiro cadastro ou renovação periódica).
     * Valida o Termo para evitar inconsistência de líderes antigos e atualiza/cadastra o nó.
     * 
     * Se a instância foi dada como morta mas enviou Heartbeat com seus dados completos,
     * o Gateway a cadastra novamente automaticamente (Auto-Recuperação).
     *
     * @param serviceName Nome do microsserviço
     * @param instanceId  Identificador único do nó
     * @param ip          Endereço IP da instância
     * @param port        Porta da instância
     * @param incomingRole Papel informado pelo nó (LEADER/FOLLOWER)
     * @param incomingTerm Termo da eleição em que o nó opera
     */
    public void processHeartbeat(String serviceName, String instanceId, String ip, int port, NodeRole incomingRole, long incomingTerm) {
        ConcurrentHashMap<String, ServiceInstance> instances = registry.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());

        // 1. Atualiza/obtém o maior termo conhecido do serviço
        long highestTermKnown = serviceTerms.compute(serviceName, (k, currentHighest) -> {
            if (currentHighest == null || incomingTerm > currentHighest) {
                return incomingTerm;
            }
            return currentHighest;
        });

        // 2. Proteção contra Líder Fantasma (Termo Antigo)
        final boolean isGhostLeader = (incomingRole == NodeRole.LEADER && incomingTerm < highestTermKnown);

        if (isGhostLeader) {
            System.err.println("[GATEWAY] Rejeitando liderança fantasma do nó " + instanceId + 
                            " (Termo recebido: " + incomingTerm + " < Termo atual: " + highestTermKnown + ")");
        }

        final NodeRole effectiveRole = isGhostLeader ? NodeRole.FOLLOWER : incomingRole;

        // 3. Se o nó é um LEADER válido, rebaixa qualquer outro líder anterior
        if (effectiveRole == NodeRole.LEADER) {
            instances.values().forEach(inst -> {
                if (!inst.getInstanceId().equals(instanceId) && inst.getRole() == NodeRole.LEADER) {
                    System.out.println("[GATEWAY] Rebaixando ex-líder " + inst.getInstanceId() + " para FOLLOWER.");
                    inst.setRole(NodeRole.FOLLOWER);
                }
            });
        }

        // 4. Upsert da instância
        instances.compute(instanceId, (String id, ServiceInstance existing) -> {
            if (existing != null) {
                existing.updateHeartbeat(effectiveRole, incomingTerm);
                return existing;
            } else {
                System.out.println("[GATEWAY] Instância Registrada/Reativada: " + instanceId + 
                                " (" + serviceName + " | IP: " + ip + ":" + port + 
                                " | Role: " + effectiveRole + " | Term: " + incomingTerm + ")");
                return new ServiceInstance(serviceName, instanceId, ip, port, effectiveRole, incomingTerm);
            }
        });
    }


    /**
     * Roteamento de ESCRITA:
     * Tenta obter o Leader do serviço.
     * Se o Leader não estiver disponível (ex: durante uma reeleição),
     * aguarda curtos intervalos (Retry/Backoff) até atingir o timeout máximo.
     * 
     * @param serviceName Nome do microsserviço ("RESERVATION", "CATALOG")
     * @param maxWaitMs Tempo máximo total de espera em milissegundos (ex: 3000ms)
     * @return Optional com a instânca do Leader ou empty se exceder o tempo
     */
    public Optional<ServiceInstance> getLeaderInstanceWithRetry(String serviceName, long maxWaitMs) {
        long startTime = System.currentTimeMillis();
        long retryIntervalMs = 500; // Tenta a cada 500ms

        while ((System.currentTimeMillis() - startTime) < maxWaitMs) {
            Optional<ServiceInstance> leader = getLeaderInstanceNow(serviceName);
            
            if (leader.isPresent()) {
                return leader;
            }

            try {
                // Aguarda 500ms antes da próxima tentativa para dar tempo do novo Leader se eleger/cadastrar
                Thread.sleep(retryIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.err.println("[GATEWAY] Timeout! Nenhum Leader encontrado para " + serviceName + " após " + maxWaitMs + "ms.");
        return Optional.empty();
    }

    /**
     * Consulta instantânea do Leader na tabela de roteamento sem espera
     */
    public Optional<ServiceInstance> getLeaderInstanceNow(String serviceName) {
        return getRegisteredInstancesSnapshot(serviceName).stream()
                .filter(inst -> inst.getRole() == NodeRole.LEADER)
                .findFirst();
    }


    /**
     * Roteamento de LEITURA:
     * Aplica Round-Robin entre TODAS as instâncias ativas do serviço (Leader + Followers)
     * para balancear a carga de consultas.
     */
    public Optional<ServiceInstance> getReadInstance(String serviceName) {
        List<ServiceInstance> activeInstances = getRegisteredInstancesSnapshot(serviceName);
        if (activeInstances.isEmpty()) {
            return Optional.empty();
        }

        AtomicInteger counter = roundRobinIndexes.computeIfAbsent(serviceName, k -> new AtomicInteger(0));
        int nextVal = counter.getAndUpdate(i -> (i >= Integer.MAX_VALUE - 1) ? 0 : i + 1);
        int index = nextVal % activeInstances.size();

        return Optional.of(activeInstances.get(index));
    }

    /**
     * Método auxiliar privado para obter a lista snapshot de instâncias vivas
     */
    public List<ServiceInstance> getRegisteredInstancesSnapshot(String serviceName) {
        Map<String, ServiceInstance> instances = registry.get(serviceName);
        if (instances == null || instances.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(instances.values());
    }

    /**
     * Remove instâncias inativas da tabela de roteamento quando o heartbeat expira.
     */
    private void purgeDeadInstances() {
        registry.forEach((serviceName, instances) -> {
            instances.entrySet().removeIf(entry -> {
                boolean isDead = entry.getValue().isExpired(HEARTBEAT_TIMEOUT_MS);
                if (isDead) {
                    System.err.println("[GATEWAY - FALHA] Instância desativada por falta de Heartbeat: " + entry.getKey());
                }
                return isDead;
            });
        });
    }

    public void stop() {
        healthChecker.shutdownNow();
    }
}
