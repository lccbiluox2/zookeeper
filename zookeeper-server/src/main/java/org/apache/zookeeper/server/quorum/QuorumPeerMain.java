/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    // 代表一台服务器
    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    /**
     *  QuorumPeer 管理和代表了一台服务器，那么QuorumPeerConfig 就一定是用来管理 配置的
     * 启动的之后，zkserver 会去加载zoo.cfg，
     * 这里面的配置，都会读取进来保存在QuorumPeerConfig 中
     *
     * @param args
     * @throws ConfigException
     * @throws IOException
     */
    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        // 解析配置， 如果传入的是配置文件(参数只有一个)，解析配置文件并初始化QuorumPeerConfig
        // 集群模式，会解析配置参数
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            // 解析参数,通过 Properties 方式来进行
            config.parse(args[0]);
        }

        /**
         * DatadirCleanupManager 线程，由于 zookeeper 的任何一个变更操作都产生事务，事务日志
         * 都需要持久化到磁盘，同时当写入操作达到一定量或者一定时间间隔后，会对内存中的数据进行
         * 一次快照，并且写入到磁盘上的snapshop中。快照为了缩短启动时加载数据的时间，从而加快
         * 整个系统启动。
         *
         * 而随着运行事件的增长生成的 transaction log 和 snapshot 将越来越多，所以要定期清理
         * DatadirCleanupManager 就是启动一个 TimeTask 定时任务用于清理 DataDir 中的
         * SnapShot 以及对应的 transaction log
         *
         * DatadirCleanupManager 主要有2个参数
         *  snapRetainCount :清理后保留的snapshot的个数，对应的 autopurge.snapRetainCount,大于等于3.默认3
         *  purgeInterval: 清理任务 TimeTask 执行周期，几个小时清理一次，对应配置 autopurge.purgeInterval 单位小时
         *
         * 因为快照文件越来越多，需要进行清理
         */

        // Start and schedule the the purge task
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        // 启动清理文件的线程
        purgeMgr.start();

        /**
         * 根据配置中的 servers 数量判断是集群环境还是单机环境，如果单机环境以standalone 模式运行
         * 直接调用 ZooKeeperServerMain. main() 方法，否则进入集群模式中。
         * 键命令格式: zkServer.sh start
         */

        // 集群模式
        if (args.length == 1 && config.servers.size() > 0) {
            /**
             * 集群模式：生成环境，毫无疑问，都是集群模式，所以重点关注集群模式
             */
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            /**
             * 单机模式
             */
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config) throws IOException {
      try {
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }
  
      LOG.info("Starting quorum peer");
      try {
          /**
           * 创建 ServerCnxnFactory 实例, ServerCnxnFactory 从名字就可以看出是一个工厂类 ServerCnxn ,
           * ServerCnxn这个类代表一个客户端与一个server 的连接， 每个客户端连接过 来都会 被封装成一个ServerCnxn 实例
           * 来维护服务器与客户端之间的Socket通道。
           *
           * 首先要有监听端口，客户端连接才能过来，ServerCnxnFactory.configure()方法的核心就是启动监昕端口供客户端连接
           * 端口号由配置文件 clientPorti属性配置，默认2181
           *
           * ServerCnxnFactory 有 NIOServerCnxnFactory 和 NettyServerCnxnFactory 2种.
           *
           * NIOServerCnxnFactory的作用: ?监听2181端口如果 有客户端发送请求过来，则由这个组件进行处理
           * 如果有一个客户端发送链接请求过来，NIOServerCnxnFactory 处理了之后， 就会生成一个 ServerCnxn来负责管理
           */
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(config.getClientPortAddress(),
                                config.getMaxClientCnxns());

          /**
           * 获取 QuorumPeer ，zk的逻辑主线程，负责选举，投票，QuorumPeer 是一个线程，注意他的 start
           * 和 run 方法，里面有一个内部类：QuorumServer
           */
          quorumPeer = getQuorumPeer();

          quorumPeer.setQuorumPeers(config.getServers());
          // FileTxnSnapLog 主要用于 snap 和 transaction log 的 IO 工具类
          quorumPeer.setTxnFactory(new FileTxnSnapLog(
                  new File(config.getDataLogDir()),
                  new File(config.getDataDir())));
          // 选举类型，用于确定选举算法
          quorumPeer.setElectionType(config.getElectionAlg());
          // 设置 myid
          quorumPeer.setMyid(config.getServerId());
          // 设置心跳时间
          quorumPeer.setTickTime(config.getTickTime());
          // 设置初始化同步时间
          quorumPeer.setInitLimit(config.getInitLimit());
          // 设置节点间状态同步时间
          quorumPeer.setSyncLimit(config.getSyncLimit());
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
          quorumPeer.setCnxnFactory(cnxnFactory);
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
          quorumPeer.setClientPortAddress(config.getClientPortAddress());
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
          // ZKDatabase 维护在zk在内存中的数据结构
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
          // 服务节点的角色类型 两种类型
          // Learner的两种类型: PARTICIPANT :有选举权和被选举权，OBSERVER 没有选举权和被选举权

          // zk服务器的类型: PARTICIPANT, OBSERVER; 只是在zoo.cfg 中用来进行配置的。
          // PARTICIPANT: LEADER, FOLLOWER
          // 只有服务器被配置成PARTICIPANT 类型，才有资格有选举权 和被选举权
          // learner: OBSERVER + FOLLOWER
          quorumPeer.setLearnerType(config.getPeerType());
          quorumPeer.setSyncEnabled(config.getSyncEnabled());

          // sets quorum sasl authentication configurations
          quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
          if(quorumPeer.isQuorumSaslAuthEnabled()){
              quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
              quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
              quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
              quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
              quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
          }

          quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
          quorumPeer.initialize();

          /**
           * 请注意:这个方法的调用完毕之后，会跳转到QuorumPeer的run()方法
           * 启动主线程，quorumPeer 重写了 Thread.start方法
           */
          quorumPeer.start();
          quorumPeer.join();
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}
