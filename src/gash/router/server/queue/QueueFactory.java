/*
 * copyright 2014, gash
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

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.WorkHandler;
import io.netty.channel.Channel;

public class QueueFactory {

	public static ChannelQueue getInstance(Channel channel, ServerState state) {
		// if a single queue is needed, this is where we would obtain a
		// handle to it.
		ChannelQueue queue = null;

		if (channel == null)
			queue = new NoOpWorkQueue();
		else {
			queue = new PerChannelWorkQueue(channel,state);
		}

		// on close remove from queue
		channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

		return queue;
	}

	public static ChannelQueue getInstance(Channel channel, RoutingConf conf) {
		// if a single queue is needed, this is where we would obtain a
		// handle to it.
		ChannelQueue queue = null;

		if (channel == null)
			queue = new NoOpWorkQueue();
		else {
			queue = new PerChannelCommandQueue(channel,conf);
		}

		// on close remove from queue
		channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

		return queue;
	}

}


