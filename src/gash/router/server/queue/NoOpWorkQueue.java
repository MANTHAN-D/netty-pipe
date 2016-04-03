package gash.router.server.queue;

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

import com.google.protobuf.GeneratedMessage;
import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import pipe.work.Work;


/**
 * an implementation that does nothing with incoming requests.
 *
 * @author gash
 *
 */
public class NoOpWorkQueue implements ChannelQueue {
    protected static Logger logger = LoggerFactory.getLogger("server");
    private String queueName;

    public NoOpWorkQueue() {
        queueName = this.getClass().getName();
    }

    @Override
    public void shutdown(boolean hard) {
        logger.info(queueName + ": queue shutting down");
    }

    @Override
    public void enqueueRequest(GeneratedMessage req, Channel notused) {
        logger.info(queueName + ": request received");
    }

    @Override
    public void enqueueResponse(GeneratedMessage reply, Channel notused) {
        logger.info(queueName + ": response received");
    }

    @Override
    public void setState(ServerState state) {
        //not to be implemented
    }

    @Override
    public void setRouteConfig(RoutingConf config) {
        //not to be implemented
    }

}
