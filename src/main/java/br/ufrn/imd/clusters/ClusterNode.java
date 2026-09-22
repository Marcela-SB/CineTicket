package br.ufrn.imd.clusters;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import br.ufrn.imd.clusters.messages.ClusterMessage;
import br.ufrn.imd.clusters.messages.HeartbeatMessage;
import br.ufrn.imd.clusters.messages.VoteRequestMessage;
import br.ufrn.imd.clusters.messages.VoteResponseMessage;
import br.ufrn.imd.enums.ClusterMessageType;
import br.ufrn.imd.enums.NodeRole;
import lombok.Getter;

@Getter 
public class ClusterNode {

    private final String serviceName = "RESERVATION"; 
    private final String gatewayIp = "IP_PRIVADO_DA_VM_DO_GATEWAY"; //MUDAR QUANDO SUBIR NA AWS
    private final int gatewayUdpPort = 9999;

    private final String nodeId;
    private final int localPort;
    private final List<InetSocketAddress> peers;
    private final ClusterEventInterface eventListener;

    private volatile NodeRole currentRole;
    private volatile String currentLeaderId;
    private final AtomicLong currentTerm = new AtomicLong(0);

    // Controle de Votação e Timeouts
    private String votedFor = null;
    private volatile long lastHeartbeatReceived;
    
    // Raft: Timeout aleatório para evitar empate de votos (Split Vote)
    private final long electionTimeoutMs;
    private static final long HEARTBEAT_INTERVAL_MS = 1000;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public ClusterNode(String nodeId, int localPort, NodeRole initialRole, List<InetSocketAddress> peers, ClusterEventInterface eventListener) {
        this.nodeId = nodeId;
        this.localPort = localPort;
        this.currentRole = initialRole;
        this.peers = peers;
        this.eventListener = eventListener;
        this.lastHeartbeatReceived = System.currentTimeMillis();

        // Gera um timeout aleatório entre 1500ms e 3000ms por instância
        this.electionTimeoutMs = 1500 + ThreadLocalRandom.current().nextInt(1500);
    }

    public void start() {
        // Inicia o servidor TCP para escutar peers
        startServer();
        // Checagem periódica do estado do nó
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkClusterState();
            } catch (Throwable t) {
                System.err.println("[" + nodeId + "] Erro no loop principal do cluster: " + t.getMessage());
            }
        }, 500, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // --- SERVIDOR TCP PARA ESCUTAR PEERS ---
    private void startServer() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                serverSocket = new ServerSocket(localPort);
                while (running) {
                    Socket socket = serverSocket.accept();
                    handleIncomingConnection(socket);
                }
     
            } catch (IOException e) {
                if (running) System.err.println("[" + nodeId + "] Erro no servidor TCP: " + e.getMessage());
            }
        });
    }

    private void handleIncomingConnection(Socket socket) {
        Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                ClusterMessage msg = (ClusterMessage) in.readObject();
                
                switch (msg.type()) {
                    case HEARTBEAT -> {
                        HeartbeatMessage hb = (HeartbeatMessage) msg.payload();
                        processReceivedHeartbeat(hb);
                    }
                    case VOTE_REQUEST -> {
                        VoteRequestMessage req = (VoteRequestMessage) msg.payload();
                        VoteResponseMessage resp = processVoteRequest(req);
                        out.writeObject(resp);
                        out.flush();
                    }
                    case REPLICATION -> {
                        byte[] payload = (byte[]) msg.payload();
                        eventListener.onDataReceived(payload);
                    }
					case VOTE_RESPONSE -> {
                        throw new UnsupportedOperationException("Unimplemented case: " + msg.type());
                    }
                    case REQUISITION -> {
                        byte[] payload = (byte[]) msg.payload();
                        // Executa a regra de negócio do Service
                        Object responsePayload = eventListener.onRequisition(payload);
                        
                        // Devolve uma ClusterMessage com a resposta para o Gateway!
                        ClusterMessage responseMsg = new ClusterMessage(
                            ClusterMessageType.REQUISITION, 
                            nodeId, 
                            currentTerm.get(), 
                            responsePayload
                        );
                        out.writeObject(responseMsg);
                        out.flush();
                    }
					default -> {
                        throw new IllegalArgumentException("Unexpected value: " + msg.type());
                    }
                }
            } catch (Exception e) {
                // Conexão encerrada ou erro de IO
            }
        });
    }

    // --- MÁQUINA DE ESTADOS PRINCIPAL ---
    private void checkClusterState() {
        // TODOS OS NÓS avisam ao Gateway que estão vivos via UDP
        sendHeartbeatToGateway();

        // APENAS O LEADER mantém a autoridade no cluster via TCP
        if (currentRole == NodeRole.LEADER) {
            sendHeartbeatToPeers();
        } else {
            // Se for FOLLOWER ou CANDIDATE, verifica timeout do Líder
            long elapsed = System.currentTimeMillis() - lastHeartbeatReceived;
            if (elapsed > electionTimeoutMs) {
                startElection();
            }
        }
    }

    /**
     * 
     */
    private void sendHeartbeatToGateway() {
        try (DatagramSocket udpSocket = new DatagramSocket()) {
            // Formato: SERVICE_NAME;INSTANCE_ID;PORT;ROLE;TERM
            String payload = String.format("%s;%s;%d;%s;%d", 
                    serviceName, nodeId, localPort, currentRole, currentTerm.get());
            
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            InetAddress gatewayAddr = InetAddress.getByName(gatewayIp);
            DatagramPacket packet = new DatagramPacket(bytes, bytes.length, gatewayAddr, gatewayUdpPort);
            
            udpSocket.send(packet);
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Erro ao notificar Gateway: " + e.getMessage());
        }
    }

    // --- LÓGICA DE ELEIÇÃO (CANDIDATE) ---
    private void startElection() {
        VoteRequestMessage voteRequest;
        int majorityQuorum;

        synchronized (this) {
            // Aborta a eleição! 
            // Não faz sentido virar Candidato se eu já sou o Líder.
            if (currentRole == NodeRole.LEADER) return;

            this.currentRole = NodeRole.CANDIDATE;
            
            long newTerm = currentTerm.incrementAndGet(); // Incrementa a era/termo do cluster
            this.votedFor = nodeId; // Vota em si mesmo
            this.lastHeartbeatReceived = System.currentTimeMillis();

            System.out.println("[" + nodeId + "] Timeout do Líder! Transicionado para CANDIDATE no Termo " + newTerm);
            eventListener.onBecameCandidate(newTerm);
            
            // Quórum necessário para eleição (Maioria simples)
            majorityQuorum = ((peers.size() + 1) / 2) + 1;
            voteRequest = new VoteRequestMessage(nodeId, newTerm);
        }

        // Voto próprio já contado
        AtomicInteger votesReceived = new AtomicInteger(1);

        // O envio assíncrono para a rede roda FORA do bloco synchronized
        for (InetSocketAddress peer : peers) {
            // Pede voto aos peers em paralelo
            asyncExecutor.submit(() -> { 
                VoteResponseMessage response = sendVoteRequest(peer, voteRequest);
                if (response != null && response.voteGranted()) {
                    if (votesReceived.incrementAndGet() >= majorityQuorum) {
                        promoteToLeader();
                    }
                }
            });
        }
    }

    private synchronized void promoteToLeader() {
        // Garante que só sobe para LEADER se ainda estiver como CANDIDATE
        if (currentRole == NodeRole.CANDIDATE) {
            this.currentRole = NodeRole.LEADER;
            this.currentLeaderId = nodeId;
            System.out.println("====== [" + nodeId + "] GANHOU A ELEIÇÃO! Novo LEADER no Termo " + currentTerm.get() + " ======");
            
            eventListener.onElectedLeader();
            sendHeartbeatToPeers(); // Envia Heartbeat imediatamente para que os outros voltem a ser FOLLOWER
        }
    }

    // --- RECEBIMENTO E TRATAMENTO DE MENSAGENS DE REDE ---

    /**
     * Processa Heartbeat vindo do Líder
     * @param msg - mensagem de Heartbeat recebida
     */
    public synchronized void processReceivedHeartbeat(HeartbeatMessage msg) {
        if (msg.getTerm() >= currentTerm.get()) {
            this.currentTerm.set(msg.getTerm());
            this.lastHeartbeatReceived = System.currentTimeMillis();
            
            if (currentRole != NodeRole.FOLLOWER || !msg.getLeaderId().equals(currentLeaderId)) {
                this.currentRole = NodeRole.FOLLOWER;
                this.currentLeaderId = msg.getLeaderId();
                this.votedFor = null; // Reseta voto para a nova era
                eventListener.onBecameFollower(currentLeaderId, currentTerm.get());
            }
        }
    }

    /**
     * Processa Pedido de Voto vindo de um CANDIDATE
     * @param req - mensagem de pedido de voto recebida
     * @return {@link VoteResponseMessage} - resposta ao pedido de voto
     */
    public synchronized VoteResponseMessage processVoteRequest(VoteRequestMessage req) {
        // Se a era da mensagem for maior, atualiza o termo local e volta a ser Follower
        if (req.term() > currentTerm.get()) {
            currentTerm.set(req.term());
            currentRole = NodeRole.FOLLOWER;
            votedFor = null;
        }

        // Concede voto se: termo é igual ao atual e o nó ainda não votou ou votou no mesmo candidato
        boolean canVote = req.term() == currentTerm.get() && (votedFor == null || votedFor.equals(req.candidateId()));

        if (canVote) {
            votedFor = req.candidateId();
            lastHeartbeatReceived = System.currentTimeMillis(); // Reseta timer pois considerou um processo legítimo
            return new VoteResponseMessage(currentTerm.get(), true);
        }

        return new VoteResponseMessage(currentTerm.get(), false);
    }


    // --- DISPARO DE MENSAGENS TCP ---

    /** Envia Heartbeat para todos os peers do cluster */
    private void sendHeartbeatToPeers() {
        HeartbeatMessage hb = new HeartbeatMessage(nodeId, currentTerm.get());
        ClusterMessage msg = new ClusterMessage(ClusterMessageType.HEARTBEAT, nodeId, currentTerm.get(), hb);
        
        for (InetSocketAddress peer : peers) {
            sendAsyncMessage(peer, msg);
        }
    }

    public void sendReplicationToPeers(byte[] payload) {
        ClusterMessage msg = new ClusterMessage(ClusterMessageType.REPLICATION, nodeId, currentTerm.get(), payload);
        for (InetSocketAddress peer : peers) {
            sendAsyncMessage(peer, msg);
        }
    }

    /**
     * Envia Pedido de Voto para um peer específico e aguarda resposta
     * @param peer - endereço do peer para enviar o pedido
     * @param req - mensagem de pedido de voto
     * @return {@link VoteResponseMessage} - resposta do peer ao pedido de voto
     */
    private VoteResponseMessage sendVoteRequest(InetSocketAddress peer, VoteRequestMessage req) {
        ClusterMessage msg = new ClusterMessage(ClusterMessageType.VOTE_REQUEST, nodeId, currentTerm.get(), req);
        try (Socket socket = new Socket()) {
            socket.connect(peer, 500); // Timeout de conexão de 500ms
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            out.writeObject(msg);
            out.flush();
            return (VoteResponseMessage) in.readObject();
        } catch (Exception e) {
            return null; // Peer indisponível ou offline
        }
    }

    private void sendAsyncMessage(InetSocketAddress peer, ClusterMessage msg) {
        asyncExecutor.submit(() -> {
            try (Socket socket = new Socket()) {
                socket.connect(peer, 300);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
            } catch (Exception ignored) {
                // Peer inativo
            }
        });
    }

    public void stop() {
        this.running = false;
        this.scheduler.shutdown();
        this.asyncExecutor.shutdown();
        if (serverSocket != null && !serverSocket.isClosed()) {
            try { serverSocket.close(); } catch (IOException ignored) {}
        }
    }
}
