package com.bjfu.distributed;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistributeClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        /*
         * 1.获取zookeeper集群连接
         * 2.注册监听
         * 3.业务逻辑处理
         */

        DistributeClient distributeClient = new DistributeClient();//先拿到一个消费者对象
        //1.获取zookeeper集群连接
        distributeClient.getConnect();
        //2.注册监听，是为了获取下面的子节点
        distributeClient.getChildren();
        //3.业务逻辑处理
        distributeClient.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {
        //这里设置为true的话，就必须在new Watcher中写代码，否则只会监听一次
        List<String> children = zooKeeperClient.getChildren("/servers", true);

        //服务端主机名称集合
        ArrayList<String> hosts = new ArrayList<>();
        for (String child : children) {
            byte[] data = zooKeeperClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        //将所有在线的服务端打印到控制台
        System.out.println(hosts);
    }

    ZooKeeper zooKeeperClient;

    private void getConnect() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        zooKeeperClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
