package gash.router.server.election;

import pipe.election.*;
import pipe.election.Election;

/**
 * Created by pranav on 4/1/16.
 */
public class ElectionState {

    protected Integer id;
    protected String desc;
    protected int version = 0;
    protected Election.LeaderStatus.LeaderQuery state = Election.LeaderStatus.LeaderQuery.DECLAREELECTION;
    protected Integer electionID;
    protected boolean active = false;
    protected ElectionListener listener;
    public boolean isActive(){return id != null && active;}
    protected int candidate;
    protected long startedOn = 0, lastVoteOn = 0, maxDuration = -1;
}
