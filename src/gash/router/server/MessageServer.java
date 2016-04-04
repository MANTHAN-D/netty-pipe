/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import gash.router.server.election.RaftManager;
import io.netty.channel.ChannelFutureListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.NoOpBalancer;
import gash.router.server.tasks.TaskList;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MessageServer implements RoutingConfSubject{//}, Runnable{
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();
	private static EdgeMonitor emon=null; // added by Manthan
	private static RaftManager mgr = null;//Pranav
	//private static ElectionManager emgr = null; // Pranav
	private ArrayList<RoutingConfObserver> routingConfOberverList;// added by Manthan

	// public static final String sPort = "port";
	// public static final String sPoolSize = "pool.size";

	protected File confFile; // added by Manthan
	protected RoutingConf conf;
	protected boolean background = false;

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public MessageServer(File cfg) {
		routingConfOberverList = new ArrayList<>();
		this.confFile = cfg;
		init(cfg);
	}

	public MessageServer(RoutingConf conf) {
		this.conf = conf;
	}

	public void release() {
	}

	public void startServer() {
		StartWorkCommunication comm = new StartWorkCommunication(conf);
		attach(comm);
		logger.info("Work starting");
		// We always start the worker in the background
		Thread cthread = new Thread(comm);
		cthread.start();

		// Start the thread that reads any updates in conf File : thread in background // Added by Manthan
		logger.info("Conf updater starting");
		Thread confUpdateThread = new Thread(new StartRoutingUpdater(this));
		confUpdateThread.start();
		//Pranav - raft
		mgr = RaftManager.initManager(conf);
		//emgr = ElectionManager.initManager(conf);
		System.out.print("Raft: " + mgr);

		if (!conf.isInternalNode()) {
			StartCommandCommunication comm2 = new StartCommandCommunication(conf);
			logger.info("Command starting");

			if (background) {
				Thread cthread2 = new Thread(comm2);
				cthread2.start();
			} else
				comm2.run();
		}

		/*// Start the thread that reads any updates in conf File : thread in background // Added by Manthan
		logger.info("Conf update thread starting");
		Thread confUpdateThread = new Thread(this);
		confUpdateThread.start();
		*/
	}

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		logger.info("Server shutdown");
		System.exit(0);
	}

	private void init(File cfg) {
		if (!cfg.exists())
			throw new RuntimeException(cfg.getAbsolutePath() + " not found");
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), RoutingConf.class);
			if (!verifyConf(conf))
				throw new RuntimeException("verification of configuration failed");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private boolean verifyConf(RoutingConf conf) {
		return (conf != null);
	}


	/**
	 * Attach observer for changes in conf file
	 *
	 * @author Manthan
	 *
	 */
	@Override
	public void attach(RoutingConfObserver observer) {
		routingConfOberverList.add(observer);
	}

	/**
	 * Detach observer for changes in conf file
	 *
	 * @author Manthan
	 *
	 */
	@Override
	public void detach(RoutingConfObserver observer) {
		routingConfOberverList.remove(observer);
	}

	/**
	 * Notify observers for changes in conf file
	 *
	 * @author Manthan
	 *
	 */
	@Override
	public void notifyObservers() {
		for(RoutingConfObserver observer : routingConfOberverList){
			observer.updateRoutingConf(conf);
		}
	}
	/**Ended by Manthan*/
	/**
	 * initialize netty communication
	 * 
	 * @param //port
	 *            The port to listen to
	 */
	private static class StartCommandCommunication implements Runnable {
		RoutingConf conf;

		public StartCommandCommunication(RoutingConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(conf.getCommandPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new GlobalCommandInit(conf, compressComm));

				// Start the server.
				logger.info("Starting command server (" + conf.getNodeId() + "), listening on port = "
						+ conf.getCommandPort());
				ChannelFuture f = b.bind(conf.getCommandPort()).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();

			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}

	/**
	 * initialize netty communication
	 *
	 *            The port to listen to
	 */
	private static class StartWorkCommunication implements Runnable, RoutingConfObserver {
		ServerState state;
		//RaftManager mgr;
		public StartWorkCommunication(RoutingConf conf) {
			if (conf == null)
				throw new RuntimeException("missing conf");

			state = new ServerState();
			state.setConf(conf);
			TaskList tasks = new TaskList(new NoOpBalancer());
			state.setTasks(tasks);

			emon = new EdgeMonitor(state);//changed by Manthan -> emon is an instance of parent class
			Thread t = new Thread(emon);
			// Pranav - RAFT

			t.start();
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(state.getConf().getWorkPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new WorkInit(state, compressComm));

				// Start the server.
				logger.info("Starting work server (" + state.getConf().getNodeId() + "), listening on port = "
						+ state.getConf().getWorkPort());
				ChannelFuture f = b.bind(state.getConf().getWorkPort()).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				mgr.startMonitor(state); // Pranav - RAFT

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
				logger.info("I am done");

			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				logger.info("Finally being executed");
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();

				// shutdown monitor
				EdgeMonitor emon = state.getEmon();
				if (emon != null)
					emon.shutdown();
				logger.info("Finally execution done");
			}
		}

		/**
		 * Update state with latest confFile
		 *
		 * @author Manthan
		 *
		 */
		@Override
		public void updateRoutingConf(RoutingConf newConf){

			state.updateRoutingConf(newConf);
			MessageServer.setEmon(state.getEmon());
		}
	}

	/**
	 * initialize netty communication
	 * @author manthan
	 */
	private static class StartRoutingUpdater implements Runnable {
		File confFile;
		//RoutingConf conf;
		MessageServer svr;

		public StartRoutingUpdater(MessageServer svr) {
			this.svr = svr;
			this.confFile = this.svr.confFile;
		}

		/**
		 * Thread to regularly read updated conf file
		 *
		 * @author Manthan
		 *
		 */
		@Override
		public void run(){

			BufferedInputStream br;
			byte[] raw;
			while(true){
				br=null;
				raw=null;
				if (!confFile.exists())
					throw new RuntimeException(confFile.getAbsolutePath() + " not found");
				// resource initialization - how message are processed

				try {
					//logger.info("Updating conf file ...");
					raw = new byte[(int) confFile.length()];
					br = new BufferedInputStream(new FileInputStream(confFile));
					br.read(raw);
					this.svr.conf = JsonUtil.decode(new String(raw), RoutingConf.class);
					if (!this.svr.verifyConf(this.svr.conf))
						throw new RuntimeException("verification of configuration failed");
					this.svr.notifyObservers();
				} catch (Exception ex) {
					ex.printStackTrace();
				} finally {
					if (br != null) {
						try {
							br.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					//make thread sleep for 3000 miliseconds
					try{
						//logger.info("Conf update sleeping...");
						Thread.sleep(3000);
					}
					catch(InterruptedException e){
						e.printStackTrace();
					}
				}
			}

		}
	}

	/**
	 * help with processing the configuration information
	 * 
	 * @author gash
	 *
	 */
	public static class JsonUtil {
		private static JsonUtil instance;

		public static void init(File cfg) {

		}

		public static JsonUtil getInstance() {
			if (instance == null)
				throw new RuntimeException("Server has not been initialized");

			return instance;
		}

		public static String encode(Object data) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.writeValueAsString(data);
			} catch (Exception ex) {
				return null;
			}
		}

		public static <T> T decode(String data, Class<T> theClass) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(data.getBytes(), theClass);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}
	}

	/**
	 * return the object of EdgeMonitor
	 *
	 * @author Manthan
	 *
	 */
	public static EdgeMonitor getEmon(){
		return emon;
	}

	/**
	 * updates the object of EdgeMonitor
	 *
	 * @author Manthan
	 *
	 */
	public static void setEmon(EdgeMonitor newEmon){
		emon = newEmon;
	}
}
