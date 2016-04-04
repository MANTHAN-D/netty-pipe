package gash.router.server.resources;

import com.google.protobuf.ByteString;
import database.dao.MongoDAO;
import database.model.DataModel;
import gash.router.server.PrintUtil;
import gash.router.server.queue.ChannelQueue;
import gash.router.server.queue.PerChannelGlobalCommandQueue;
import global.Global;
import pipe.work.Work;
import routing.Pipe;
import storage.Storage;

import java.util.ArrayList;

/**
 * Created by rushil on 4/3/16.
 */
public class Query extends Resource {

    Storage.Query query;

    public Query(ChannelQueue sq){
        super(sq);
    }


    public void handleGlobalCommand(Global.GlobalCommandMessage msg) {

        query = msg.getQuery();
        //If this have to handle on the same node
        //TODO: change the logic so that it has to be dependent on configuration and intra cluster node space dependent.
        if (msg.getHeader().getDestination() == ((PerChannelGlobalCommandQueue) sq).getRoutingConf().getNodeId()) {
            switch (query.getAction()) {
                case GET:
                    PrintUtil.printGlobalCommand(msg);
                    /*ArrayList<MongoDataModel> arrRespData = MongoDAO.getData("temp",new MongoDataModel(query.getKey(),null));
                    for(MongoDataModel dataModel : arrRespData){
                        Storage.Response.Builder rb = Storage.Response.newBuilder();
                        rb.setAction(Storage.Action.GET);
                        rb.setSuccess(true);
                        rb.setKey(dataModel.getName());
                        rb.setSequenceNo(dataModel.getSeqNumber());
                        rb.setData(ByteString.copyFrom(dataModel.getDataChunk()));
                    }*/
                    break;
                case STORE:
                    /*int result = MongoDAO.saveData("temp",new MongoDataModel(query.getKey(),query.getSequenceNo(),query.getData().toByteArray()));
                    Storage.Response.Builder rb = Storage.Response.newBuilder();
                    rb.setAction(Storage.Action.GET);
                    rb.setSuccess(result>0);*/
                    PrintUtil.printGlobalCommand(msg);
                    break;
                case UPDATE:
                case DELETE:
                    break;
            }

        }
    }

    public void handleCommand(Pipe.CommandRequest msg) {
        //Not to be implement
    }

    public void handleWork(Work.WorkRequest msg) {
        Storage.Query query = msg.getPayload().getQuery();
        logger.error("Query from " + msg.getHeader().getNodeId());
    }


}
