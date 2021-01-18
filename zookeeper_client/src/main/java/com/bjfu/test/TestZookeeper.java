package com.bjfu.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestZookeeper {
    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    ZooKeeper zooKeeperClient;

    @Before
    public void init() throws IOException {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("========start=========");
                List<String> children;
                try {
                    children = zooKeeperClient.getChildren("/", true);
                    for (String child :
                            children) {
                        System.out.println(child);
                    }
                    System.out.println("========end=========");
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        zooKeeperClient = new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    /**
     * 1.创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zooKeeperClient.create("/A", "A's data".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * 2.获取子节点并监听节点变化
     */
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        //如果要监听的话，光设置为true也不行，还得在Watcher中的process也写这一大段代码
        List<String> children = zooKeeperClient.getChildren("/", true);
        for (String child :
                children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat exists = zooKeeperClient.exists("/", false);
        System.out.println(exists);
    }
}
