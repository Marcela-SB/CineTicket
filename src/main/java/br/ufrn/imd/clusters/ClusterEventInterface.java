package br.ufrn.imd.clusters;

public interface ClusterEventInterface {
    /**
     * Disparado quando recebe requisições do cliente (vindas do Gateway).
     * 
     * @param payload
     * @return
     */
    Object onRequisition(byte[] payload);

    /** Disparado quando este nó é promovido a LEADER */
    void onElectedLeader();

    /** Disparado quando este nó entra em estado de eleição (CANDIDATE) */
    void onBecameCandidate(long term);

    /** Disparado quando o nó passa a ser FOLLOWER de algum Leader */
    void onBecameFollower(String leaderId, long currentTerm);

    /** Recebe os payloads de replicação de dados */
    void onDataReceived(byte[] payload);
}