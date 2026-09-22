package br.ufrn.imd.clusters.messages;

import java.io.Serializable;

import br.ufrn.imd.enums.ClusterMessageType;

/**
 * ClusterMessage
 */
public record ClusterMessage(
    ClusterMessageType type,
    String senderId,
    long term,
    Object payload
) implements Serializable {}
