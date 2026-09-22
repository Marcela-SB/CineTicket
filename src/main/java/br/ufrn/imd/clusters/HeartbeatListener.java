package br.ufrn.imd.clusters;

import br.ufrn.imd.api.ClusterManager;
import br.ufrn.imd.enums.NodeRole;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

/**
 * Classe que inicializa o servidor UDP interno para escutar os heartbeats enviados pelas intâncias ao Gateway
 */
public class HeartbeatListener implements Runnable {
    private final int port;
    private final ClusterManager clusterManager;
    private volatile boolean running = true;

    public HeartbeatListener(int port, ClusterManager clusterManager) {
        this.port = port;
        this.clusterManager = clusterManager;
    }

    @Override
    public void run() {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            System.out.println("[Gateway HeartbeatListener] Escutando Heartbeats na porta UDP " + port);
            byte[] buffer = new byte[1024];

            while (running) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                // Formato esperado da mensagem UDP: "SERVICE_NAME;INSTANCE_ID;PORT;ROLE;TERM"
                // Exemplo: "RESERVATION;res-node-1;8081;LEADER,6"
                String[] parts = message.split(";");
                // Ajuste no HeartbeatListener.java:
                if (parts.length == 5) {
                    String serviceName = parts[0];
                    String instanceId = parts[1];
                    int instancePort = Integer.parseInt(parts[2]);
                    NodeRole role = NodeRole.valueOf(parts[3]);
                    long term = Long.parseLong(parts[4]);
                    String ipAddress = packet.getAddress().getHostAddress();

                    clusterManager.processHeartbeat(serviceName, instanceId, ipAddress, instancePort, role, term);
                }
            }
        } catch (Exception e) {
            if (running) System.err.println("[HeartbeatListener] Erro: " + e.getMessage());
        }
    }

    public void stop() {
        this.running = false;
    }
}