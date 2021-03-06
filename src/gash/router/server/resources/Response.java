package gash.router.server.resources;

import com.google.protobuf.GeneratedMessage;
import database.dao.MongoDAO;
import database.model.DataModel;
import gash.router.server.MessageServer;
import gash.router.server.PrintUtil;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.queue.ChannelQueue;
import gash.router.server.queue.PerChannelGlobalCommandQueue;
import gash.router.server.queue.PerChannelWorkQueue;
import global.Global;
import pipe.common.Common;
import pipe.work.Work;
import routing.Pipe;
import storage.Storage;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by rushil on 4/3/16.
 */
public class Response extends Resource {

    Storage.Response response;

    public Response(ChannelQueue sq){
        super(sq);
    }

    public void handleGlobalCommand(Global.GlobalCommandMessage msg) {

        response = msg.getResponse();

        switch (response.getAction()){
            case GET:
                break;
            case STORE:
                break;
            case UPDATE:
            case DELETE:
                break;
        }

    }

    public void handleCommand(Pipe.CommandRequest msg) {
        //Not to be implement
    }

    public void handleWork(Work.WorkRequest msg) {
        response = msg.getPayload().getResponse();
        logger.debug("Response on work channel from " + msg.getHeader().getNodeId());

        switch (response.getAction()) {
            case GET:
                PrintUtil.printWork(msg);
                forwardResponseOntoIncomingChannel(msg,false);
                break;
            case STORE:
                //todo
                PrintUtil.printWork(msg);
                forwardResponseOntoIncomingChannel(msg,false);
                break;
            case UPDATE:
            case DELETE:
                break;
        }
    }

    /**
     * Author : Manthan
     * */
    private void forwardResponseOntoIncomingChannel(GeneratedMessage msg, boolean glabalCommandMessage){

        Common.Header.Builder hb = Common.Header.newBuilder();

        if(glabalCommandMessage){

            /*Global.GlobalCommandMessage clientMessage = (Global.GlobalCommandMessage) msg;
            Global.GlobalCommandMessage.Builder cb = Global.GlobalCommandMessage.newBuilder(); // message to be returned to actual client
                hb.setTime(m);
            hb.setNodeId(((PerChannelGlobalCommandQueue) sq).getRoutingConf().getNodeId());
            hb.setDestination(clientMessage.getHeader().getDestination());// wont be available in case of request from client. but can be determined based on log replication feature
            hb.setSourceHost(Integer.toString(((PerChannelGlobalCommandQueue) sq).getRoutingConf().getNodeId()));
            hb.setDestinationHost(clientMessage.getHeader().getSourceHost()); // would be used to return message back to client

            cb.setHeader(hb);
            cb.setResponse(responseMsg); // set the reponse to the client
            ((PerChannelGlobalCommandQueue) sq).enqueueResponse(cb.build(),null);*/
        }
        else{
            Work.WorkRequest clientMessage = (Work.WorkRequest) msg;

            if(!clientMessage.getHeader().getSourceHost().contains("_")){

                Global.GlobalCommandMessage.Builder cb = Global.GlobalCommandMessage.newBuilder(); // message to be returned to actual client
                hb.setTime(((Work.WorkRequest) msg).getHeader().getTime());
                hb.setNodeId(((PerChannelGlobalCommandQueue) sq).getRoutingConf().getNodeId());
                hb.setDestination(clientMessage.getHeader().getDestination());// wont be available in case of request from client. but can be determined based on log replication feature
                hb.setSourceHost(Integer.toString(((PerChannelGlobalCommandQueue) sq).getRoutingConf().getNodeId()));
                hb.setDestinationHost(clientMessage.getHeader().getDestinationHost()); // would be used to return message back to client

                cb.setHeader(hb);
                cb.setResponse(((Work.WorkRequest) msg).getPayload().getResponse()); // set the reponse to the client
                Iterator<EdgeInfo> inBoundEdgeListIt = MessageServer.getEmon().getInboundEdgeInfoList().iterator();
                while (inBoundEdgeListIt.hasNext()) {
                    EdgeInfo ei = inBoundEdgeListIt.next();
                    if (ei.isClientChannel() && ei.getRef() == Integer.parseInt(clientMessage.getHeader().getSourceHost())) { // look for client specific channel
                        ei.getQueue().enqueueResponse(cb.build(), null);
                    } else {
                        // connection to client from where request came is down. // need to handle yet
                    }
                }
            }
            else {

                Work.WorkRequest.Builder wb = Work.WorkRequest.newBuilder(); // message to be returned

                hb.setNodeId(((PerChannelWorkQueue) sq).gerServerState().getConf().getNodeId());
                hb.setTime(((Work.WorkRequest) msg).getHeader().getTime());
                hb.setDestination(Integer.parseInt(clientMessage.getHeader().getSourceHost().substring(0, clientMessage.getHeader().getSourceHost().indexOf('_'))));// wont be available in case of request from client. but can be determined based on log replication feature
                hb.setSourceHost(clientMessage.getHeader().getSourceHost().substring(clientMessage.getHeader().getSourceHost().indexOf('_') + 1));
                hb.setDestinationHost(clientMessage.getHeader().getDestinationHost()); // would be used to return message back to client
                hb.setMaxHops(((Work.WorkRequest) msg).getHeader().getMaxHops() - 1);

                wb.setHeader(hb);
                wb.setSecret(1234567809);
                wb.setPayload(Work.Payload.newBuilder().setResponse(((Work.WorkRequest) msg).getPayload().getResponse())); // set the reponse to the client

                Iterator<EdgeInfo> inBoundEdgeListIt = MessageServer.getEmon().getInboundEdgeInfoList().iterator();
                while (inBoundEdgeListIt.hasNext()) {
                    EdgeInfo ei = inBoundEdgeListIt.next();
                    if (ei.getRef() == hb.getDestination()) {
                        ei.getQueue().enqueueResponse(wb.build(), null);
                    } else {
                        // connection from where request came is down. // need to handle yet
                    }
                }
            }

        }
    }
}
