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
import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.State;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A server queue exists for each connection (channel). A per-channel queue
 * isolates clients. However, with a per-client model. The server is required to
 * use a master scheduler/coordinator to span all queues to enact a QoS policy.
 * 
 * How well does the per-channel work when we think about a case where 1000+
 * connections?
 * 
 * @author gash
 * 
 */
public class PerChannelWorkQueue implements ChannelQueue {
	protected static Logger logger = LoggerFactory.getLogger("server");

	// The queues feed work to the inboundWork and outboundWork threads (workers). The
	// threads perform a blocking 'get' on the queue until a new event/task is
	// enqueued. This design prevents a wasteful 'spin-lock' design for the
	// threads
	//
	// Note these are directly accessible by the workers
	LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> inbound;
	LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	Channel channel;
	ServerState state;

	// This implementation uses a fixed number of threads per channel
	private WorkOutboundAppWorker oworker;
	private WorkInboundAppWorker iworker;

	// not the best method to ensure uniqueness
	private ThreadGroup tgroup = new ThreadGroup("PerChannelQ-" + System.nanoTime());

	public PerChannelWorkQueue(Channel channel,ServerState state) {
		this.channel = channel;
		this.state = state;
		init();
	}

	protected void init() {
		inbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		logger.info("Starting to listen to Command worker");
		iworker = new WorkInboundAppWorker(tgroup, 1, this);
		iworker.start();

		oworker = new WorkOutboundAppWorker(tgroup, 1, this);
		oworker.start();

		// let the handler manage the queue's shutdown
		// register listener to receive closing of channel
		// channel.getCloseFuture().addListener(new CloseListener(this));
	}

	protected Channel getChannel() {
		return channel;
	}

	protected ServerState gerServerState(){
		return state;
	}

	public void setState(ServerState state) {
		this.state = state;
	}

	@Override
	public void setRouteConfig(RoutingConf config) {
		//Nothing to do with this class
	}

	/*
             * (non-Javadoc)
             *
             * @see poke.server.ChannelQueue#shutdown(boolean)
             */
	@Override
	public void shutdown(boolean hard) {
		logger.info("server is shutting down");

		channel = null;

		if (hard) {
			// drain queues, don't allow graceful completion
			inbound.clear();
			outbound.clear();
		}

		if (iworker != null) {
			iworker.forever = false;
			if (iworker.getState() == State.BLOCKED || iworker.getState() == State.WAITING)
				iworker.interrupt();
			iworker = null;
		}

		if (oworker != null) {
			oworker.forever = false;
			if (oworker.getState() == State.BLOCKED || oworker.getState() == State.WAITING)
				oworker.interrupt();
			oworker = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueRequest(eye.Comm.Finger)
	 */
	@Override
	public void enqueueRequest(GeneratedMessage req, Channel notused) {
		try {
			inbound.put(req);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueResponse(eye.Comm.Response)
	 */
	@Override
	public void enqueueResponse(GeneratedMessage reply, Channel notused) {
		if (reply == null)
			return;

		try {
			outbound.put(reply);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	public class CloseListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public CloseListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			sq.shutdown(true);
		}
	}

	public LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> getInbound() {
		return inbound;
	}

	public LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> getOutbound() {
		return outbound;
	}
}
