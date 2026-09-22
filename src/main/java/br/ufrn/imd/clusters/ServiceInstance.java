package br.ufrn.imd.clusters;

import br.ufrn.imd.enums.NodeRole;
import lombok.Getter;

@Getter 
public class ServiceInstance {
    private final String serviceName; // Ex: "RESERVATION" ou "CATALOG"
    private final String instanceId;  // Ex: "res-node-1"
    private final String ipAddress;   // Ex: "172.31.10.5" ou "localhost"
    private final int port;           // Ex: 8081
    private NodeRole role;            // LEADER ou FOLLOWER
    private long lastHeartbeat;
    private volatile long currentTerm;

    public ServiceInstance(String serviceName, String instanceId, String ipAddress, int port, NodeRole role, long currentTerm) {
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.ipAddress = ipAddress;
        this.port = port;
        this.role = role;
        this.lastHeartbeat = System.currentTimeMillis();
        this.currentTerm = currentTerm;
    }

    /**
     * Atualiza o carimbo de data/hora do último heartbeat para o momento atual e redefine o papel da instância no cluster.
     *
     * @param role novo papel assumido pela instância no cluster
     */
    public void updateHeartbeat(NodeRole newRole, long term) {
        this.lastHeartbeat = System.currentTimeMillis();
        
        // Só aceita atualização de Role se o termo for MAIOR ou IGUAL ao que já conhecemos
        if (term >= this.currentTerm) {
            this.currentTerm = term;
            this.role = newRole;
        }
    }

    /**
     * Verifica se o último heartbeat enviado ultrapassou o tempo limite.
     *
     * @param timeoutMs tempo limite de inatividade em milissegundos
     * @return {@code true} se o tempo limite foi excedido; {@code false} caso contrário
     */
    public boolean isExpired(long timeoutMs) {
        return (System.currentTimeMillis() - lastHeartbeat) > timeoutMs;
    }


    public void setRole(NodeRole role){
        this.role = role;
    }
}
