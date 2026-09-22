package br.ufrn.imd.clusters.messages;

import java.io.Serializable;

/**
 * Resposta ao pedido de voto enviado pelos FOLLOWERs
 */
public record VoteResponseMessage(long term, boolean voteGranted) implements Serializable {
    private static final long serialVersionUID = 1L;
}