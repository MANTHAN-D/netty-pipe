/*
 * copyright 2015, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.MessageServer;
import gash.router.server.edges.EdgeInfo;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.work.Work;
import routing.Pipe;

public class CommandInboundAppWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("server");

	int workerId;
	PerChannelCommandQueue sq;
	boolean forever = true;

	public CommandInboundAppWorker(ThreadGroup tgrp, int workerId, PerChannelCommandQueue sq) {
		super(tgrp, "inboundWork-" + workerId);
		this.workerId = workerId;
		this.sq = sq;

		if (sq.inboundWork == null)
			throw new RuntimeException("connection worker detected null inboundWork queue");
	}

	@Override
	public void run() {
		Channel conn = sq.getChannel();
		if (conn == null || !conn.isOpen()) {
			logger.error("connection missing, no inboundWork communication");
			return;
		}

		while (true) {
			if (!forever && sq.inboundWork.size() == 0)
				break;

			try {
				// block until a message is enqueued
				GeneratedMessage msg = sq.inboundWork.take();

				// process request and enqueue response
				if(msg instanceof Pipe.CommandRequest){

					//PrintUtil.printCommand((Pipe.CommandRequest) msg);
					boolean msgDropFlag;

					Pipe.CommandRequest req = ((Pipe.CommandRequest) msg);
					Pipe.Payload payload = req.getPayload();
					if (payload.hasPing()) {
						logger.info("ping from " + req.getHeader().getNodeId());
						if(req.getHeader().getDestinationHost().equals(Integer.toString( sq.getRoutingConf().getCommandPort()))){
							//handle message by self
							System.out.println("Message for me: "+ payload.getMessage() + " from "+ req.getHeader().getSourceHost());
						}
						else{ //message doesn't belong to current node. Forward on other edges
							msgDropFlag = true;
							if(MessageServer.getEmon() != null){// forward if Comm-worker port is active
								for(EdgeInfo ei :MessageServer.getEmon().getOutboundEdgeInfoList()){
									if(ei.isActive() && ei.getChannel() != null){// check if channel of outboundWork edge is active
										Work.WorkRequest.Builder wb = Work.WorkRequest.newBuilder();
										wb.setHeader(req.getHeader());
										wb.setSecret(1234567809);
										wb.setPayload(Work.Payload.newBuilder().setPing(true));
										Work.WorkRequest work = wb.build();
										msgDropFlag = false;
										PerChannelWorkQueue edgeQueue = (PerChannelWorkQueue) ei.getQueue();
										edgeQueue.enqueueResponse(work,ei.getChannel());
										logger.info("Workmessage sent");
									}
								}
								if(msgDropFlag)
									logger.info("Message dropped <node,message>: <" + req.getHeader().getNodeId()+"," + payload.getMessage()+">");
							}
							else{// drop the message or queue it for limited time to send to connected node
								//todo
							}

						}
					} else if (payload.hasMessage()) {
						logger.info(payload.getMessage());
					}

				}
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected processing failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}
	}
}