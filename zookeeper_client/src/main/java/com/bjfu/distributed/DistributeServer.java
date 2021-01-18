package com.bjfu.distributed;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 服务端的代码
 */
public class DistributeServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        /*
         * 1.连接服务器集群
         * 2.注册节点
         * 3.业务逻辑处理
         */

        //连接服务器集群
        DistributeServer distributeServer = new DistributeServer();
        //getConnect()是自己定义的
        distributeServer.getConnect();
        //注册节点。注册说白了就是创建一个节点
        //distributeServer.register(args[0]);
        distributeServer.register("hadoop103");
        //业务逻辑处理
        distributeServer.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    //注册就相当于向集群中写数据
    private void register(String hostname) throws KeeperException, InterruptedException {
        //之所以用这个CreateMode.EPHEMERAL_SEQUENTIAL，是因为这样这个服务端每次启动都可以正常启动，
        //不会因为在同一个路径下被占用，所以用带序号的。另外，服务下线要想被消费者感知到，就使用临时的
        String path = zooKeeperClient.create("/servers/server", hostname.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online");
    }

    private ZooKeeper zooKeeperClient;

    private void getConnect() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        zooKeeperClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
