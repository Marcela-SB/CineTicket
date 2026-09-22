package br.ufrn.imd.clusters.messages;

import java.io.Serializable;

/**
 * Pedido de voto enviado pelo CANDIDATE
 */
public record VoteRequestMessage(String candidateId, long term) implements Serializable {
    private static final long serialVersionUID = 1L;
}
