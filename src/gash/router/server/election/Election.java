package gash.router.server.election;

import pipe.work.Work;

/**
 * Created by pranav on 4/1/16.
 */
public interface Election {
    /**
     * notification of the election results.
     *
     * TODO do we need it to be a list?
     *
     * @param listener
     */
    void setListener(ElectionListener listener);

    /**
     * reset the election
     */
    void clear();

    /**
     * Is an election currently in progress
     *
     * @return
     */
    boolean isElectionInprogress();

    /**
     * the current election's ID
     *
     * @return
     */
    Integer getElectionId();

    /**
     * create the election ID for messaging
     *
     * @return
     */
    Integer createElectionID();

    /**
     * The winner of the election
     *
     * @return The winner or null
     */
    Integer getWinner();

    /**
     * implementation of the Chang Roberts election. This assumes a
     * unidirectional closed overlay network.
     *
     * @param req
     * @return the resulting management action (if any)
     */
    Work.WorkRequest process(Work.WorkRequest req);

    /**
     * the node ID of myself.
     *
     * @param nodeId
     *            The ID of a node - this not allowed to be null!
     */
    void setNodeId(int nodeId);

}
