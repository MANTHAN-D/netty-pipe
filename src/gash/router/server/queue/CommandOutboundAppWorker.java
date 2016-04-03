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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandOutboundAppWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("server");

	int workerId;
	PerChannelCommandQueue sq;
	boolean forever = true;

	public CommandOutboundAppWorker(ThreadGroup tgrp, int workerId, PerChannelCommandQueue sq) {
		super(tgrp, "outboundWork-" + workerId);
		this.workerId = workerId;
		this.sq = sq;

		if (sq.outboundWork == null)
			throw new RuntimeException("connection worker detected no outboundWork queue");
	}

	@Override
	public void run() {
		Channel conn = sq.channel;
		if (conn == null || !conn.isOpen()) {
			PerChannelWorkQueue.logger.error("connection missing, no outboundWork communication");
			return;
		}

		while (true) {
			if (!forever && sq.outboundWork.size() == 0)
				break;

			try {
				// block until a message is enqueued
				GeneratedMessage msg = sq.outboundWork.take();
				if (conn.isWritable()) {
					boolean rtn = false;
					if (sq.channel != null && sq.channel.isOpen() && sq.channel.isWritable()) {
						
						ChannelFuture cf = sq.channel.writeAndFlush(msg);
					
						
						
						System.out.println("Server--sending -- response");
						// blocks on write - use listener to be async
						cf.awaitUninterruptibly();
						System.out.println("Written to channel");
						rtn = cf.isSuccess();
						if (!rtn) {
							System.out.println("Sending failed " + rtn
									+ "{Reason:" + cf.cause() + "}");
							sq.outboundWork.putFirst(msg);
						}
						else
							System.out.println("Message Send");
					}

				} else
					sq.outboundWork.putFirst(msg);
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected communcation failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}
	}
}
