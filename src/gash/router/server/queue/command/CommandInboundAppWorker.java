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
package gash.router.server.queue.command;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.MessageServer;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.queue.work.PerChannelWorkQueue;
import gash.router.server.resources.Ping;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
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


					Pipe.CommandRequest req = ((Pipe.CommandRequest) msg);
					Pipe.Payload payload = req.getPayload();

					if (payload.hasPing()) {
						new Ping(sq).handle(req);
					} else if (payload.hasMessage()) {
						logger.info(payload.getMessage());
					}
					else{
						//todo
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
			logger.info("Command incoming connection queue closing");
		}
	}
}