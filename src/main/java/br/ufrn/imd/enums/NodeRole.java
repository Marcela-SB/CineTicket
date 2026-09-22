package br.ufrn.imd.enums;

/**
 * Define o papel funcional na arquitetura.
 * Pode ser Leader, Follower ou Candidate.
 */
public enum NodeRole {
    LEADER,
    FOLLOWER,
    CANDIDATE // Para eleição de líder
}