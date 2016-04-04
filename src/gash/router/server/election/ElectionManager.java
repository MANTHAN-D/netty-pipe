/*
package gash.router.server.election;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Beans;
import java.util.concurrent.atomic.AtomicReference;
import gash.router.server.election.RaftElection.*;
import pipe.common.Common;
import pipe.work.Work;

*
 * Created by pranav on 4/2/16.


public class ElectionManager implements ElectionListener {
    protected static Logger logger = LoggerFactory.getLogger("election");
    protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();

    private static RoutingConf conf;
    private static long lastKnownBeat = System.currentTimeMillis();

    // number of times we try to get the leader when a node starts up
    private int firstTime = 2;

* The election that is in progress - only ONE!

    private Election election;
    private int electionCycle = -1;
    private Integer syncPt = 1;

* The leader

    Integer leaderNode;

    public static ElectionManager initManager(RoutingConf conf) {
        ElectionManager.conf = conf;
        instance.compareAndSet(null, new ElectionManager());
        return instance.get();
    }

    public static RoutingConf getConf() {
        return conf;
    }

*
     * Access a consistent instance for the life of the process.
     *
     * TODO do we need to have a singleton for this? What happens when a process
     * acts on behalf of separate interests?
     *
     * @return


    public static ElectionManager getInstance() {
        // TODO throw exception if not initialized!
        return instance.get();
    }

*
     * returns the leader of the network
     *
     * @return


    public Integer whoIsTheLeader() {
        return this.leaderNode;
    }

*
     * initiate an election from within the server - most likely scenario is the
     * heart beat manager detects a failure of the leader and calls this method.
     *
     * Depending upon the algo. used (bully, flood, lcr, hs) the
     * manager.startElection() will create the algo class instance and forward
     * processing to it. If there was an election in progress and the election
     * ID is not the same, the new election will supplant the current (older)
     * election. This should only be triggered from nodes that share an edge
     * with the leader.


    public void startElection() {
        electionCycle = electionInstance().createElectionID();
        ((RaftElection) election).setCurrentState(RState.Candidate);
        pipe.election.Election.LeaderElection.Builder elb = pipe.election.Election.LeaderElection.newBuilder();
        elb.setElectId(electionCycle);
        elb.setAction(pipe.election.Election.LeaderElection.ElectAction.DECLAREELECTION);
        elb.setDesc("Node " + conf.getNodeId() + " detects no leader. Election!");
        elb.setCandidateId(conf.getNodeId()); // promote self
        elb.setExpires(2 * 60 * 1000 + System.currentTimeMillis()); // 1 minute

        // bias the voting with my number of votes (for algos that use vote
        // counting)

        // TODO use voting int votes = conf.getNumberOfElectionVotes();

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setTime(System.currentTimeMillis());

        Common.VectorClock.Builder rpb = Common.VectorClock.newBuilder();
        rpb.setNodeId(conf.getNodeId());
        rpb.setTime(hb.getTime());
        rpb.setVersion(electionCycle);
        hb.addPath(rpb);

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb.build());
        wb.setElection(elb.build());
        wb.setSecret(12345678);
        // now send it out to all my edges
        logger.info("Election started by node " + conf.getNodeId());
        for(EdgeInfo ei: EdgeMonitor.getInstance().getOutboundEdgeInfoList())
        {
            if(ei.getChannel() !=null && ei.isActive())
            {
                ei.getChannel().writeAndFlush(wb.build());
            }
        }
    }

*
     * @param args


    public void processRequest(Work.WorkMessage msg) {
//		if (!mgmt.hasElection())
//			return;

        //LeaderElection req = mgmt.getElection();

        // when a new node joins the network it will want to know who the leader
        // is - we kind of ram this request-response in the process request
        // though there has to be a better place for it
//		if (req.getAction().getNumber() == LeaderElection.ElectAction.WHOISTHELEADER_VALUE) {
//			respondToWhoIsTheLeader(mgmt);
//			return;
//		} else if (req.getAction().getNumber() == LeaderElection.ElectAction.THELEADERIS_VALUE) {
//			logger.info("Node " + conf.getNodeId() + " got an answer on who the leader is. Its Node "
//					+ req.getCandidateId());
//			this.leaderNode = req.getCandidateId();
//			return;
//		}
//
//		// else fall through to an election
//
//		if (req.hasExpires()) {
//			long ct = System.currentTimeMillis();
//			if (ct > req.getExpires()) {
//				// ran out of time so the election is over
//				election.clear();
//				return;
//			}
//		}

        Work.WorkMessage rtn = electionInstance().process(msg);
        if (rtn != null)
            writeAndFlush(msg);
    }

    public void writeAndFlush(Work.WorkMessage msg)
    {
        for(EdgeInfo ei:EdgeMonitor.getInstance().getOutboundEdgeInfoList())
        {
            if(ei.isActive() && ei.getChannel() != null)
            {
                ei.getChannel().writeAndFlush(msg);
            }
        }
    }

*
     * check the health of the leader (usually called after a HB update)
     *
     * @param mgmt


    public void assessCurrentState() {
        // logger.info("ElectionManager.assessCurrentState() checking elected leader status");

        if (firstTime > 0 && EdgeMonitor.getInstance().getOutboundEdgeInfoList().size() > 0) {
            // give it two tries to get the leader
            this.firstTime--;
            askWhoIsTheLeader();
        } else if (leaderNode == null && (election == null || !election.isElectionInprogress())) {
            // if this is not an election state, we need to assess the H&S of
            // the network's leader
            synchronized (syncPt) {
                long now = System.currentTimeMillis();
                if(now-lastKnownBeat>1000)
                    startElection();
            }
        }
    }

* election listener implementation

    @Override
    public void concludeWith(boolean success, Integer leaderID) {
        if (success) {
            logger.info("----> the leader is " + leaderID);
            this.leaderNode = leaderID;
        }

        election.clear();
    }

    private void respondToWhoIsTheLeader(Work.WorkMessage msg) {
        if (this.leaderNode == null) {
            logger.info("----> I cannot respond to who the leader is! I don't know!");
            return;
        }

        logger.info("Node " + conf.getNodeId() + " is replying to " + msg.getHeader().getNodeId()
                + "'s request who the leader is. Its Node " + this.leaderNode);

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setTime(System.currentTimeMillis());

        Common.VectorClock.Builder rpb = Common.VectorClock.newBuilder();
        rpb.setNodeId(conf.getNodeId());
        rpb.setTime(hb.getTime());
        rpb.setVersion(electionCycle);
        hb.addPath(rpb);

        pipe.election.Election.LeaderElection.Builder elb = pipe.election.Election.LeaderElection.newBuilder();
        elb.setElectId(electionCycle);
        elb.setAction(pipe.election.Election.LeaderElection.ElectAction.THELEADERIS);
        elb.setDesc("Node " + this.leaderNode + " is the leader");
        elb.setCandidateId(this.leaderNode);
        elb.setExpires(-1);

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb.build());
        wb.setElection(elb.build());

        // now send it to the requester
        logger.info("Election started by node " + conf.getNodeId());
        try {
           // EdgeMonitor.getConnection(msg.getHeader().getNodeId()).writeAndFlush(wb.build());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void askWhoIsTheLeader() {
        logger.info("Node " + conf.getNodeId() + " is searching for the leader");
        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setTime(System.currentTimeMillis());

        Common.VectorClock.Builder rpb = Common.VectorClock.newBuilder();
        rpb.setNodeId(conf.getNodeId());
        rpb.setTime(hb.getTime());
        rpb.setVersion(electionCycle);
        hb.addPath(rpb);

        pipe.election.Election.LeaderElection.Builder elb = pipe.election.Election.LeaderElection.newBuilder();
        elb.setElectId(-1);
        elb.setAction(pipe.election.Election.LeaderElection.ElectAction.WHOISTHELEADER);
        elb.setDesc("Node " + this.leaderNode + " is asking who the leader is");
        elb.setCandidateId(-1);
        elb.setExpires(-1);

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb.build());
        wb.setElection(elb.build());

        // now send it to the requester
        writeAndFlush(wb.build());

    }

    private Election electionInstance() {
        if (election == null) {
            synchronized (syncPt) {
                if (election !=null)
                    return election;

                // new election
                String clazz = ElectionManager.conf.getElectionImplementation();

                // if an election instance already existed, this would
                // override the current election
                try {
                    election = (Election) Beans.instantiate(this.getClass().getClassLoader(), clazz);
                    election.setNodeId(conf.getNodeId());
                    election.setListener(this);

                } catch (Exception e) {
                    logger.error("Failed to create " + clazz, e);
                }
            }
        }

        return election;

    }

    public void startMonitor()
    {
        logger.info("In Raft Monitor");
        if(election==null)
            ((RaftElection)electionInstance()).getMonitor().start();

    }

}
*/
