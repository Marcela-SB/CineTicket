package br.ufrn.imd.clusters.messages;

import java.io.Serializable;

import lombok.Getter;

@Getter 
public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String leaderId;
    private final long term; // Número da era/termo atual (importante para consistência)
    private final long timestamp;

    public HeartbeatMessage(String leaderId, long term) {
        this.leaderId = leaderId;
        this.term = term;
        this.timestamp = System.currentTimeMillis();
    }
}