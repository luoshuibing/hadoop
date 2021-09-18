package com.jd.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 15:31
 */
public class MyZkClient {

    private ZooKeeper zkClient;

    public static final String URL = "hadoop144:2181,hadoop145:2181,hadoop146:2181";

    public static final int SESSION_TIMEOUT = 2000;

    @Before
    public void before() throws Exception {
        zkClient = new ZooKeeper(URL, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("默认回调函数");
            }
        });
    }

    @Test
    public void ls() throws Exception {
        // List<String> children = zkClient.getChildren("/", true);
        List<String> children = zkClient.getChildren("/", e -> {
            System.out.println("自定义回调函数");
        });
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void create() throws Exception {
        String result = zkClient.create("/a/1", "abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(result);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getPath() throws Exception {
        byte[] data = zkClient.getData("/abcd", true, new Stat());
        String result = new String(data);
        System.out.println(result);
    }

    @Test
    public void set() throws Exception {
        Stat stat = zkClient.setData("/efa", "hello".getBytes(), 0);
        int dataLength = stat.getDataLength();
        System.out.println(dataLength);
    }

    @Test
    public void stat() throws Exception {
        Stat result = zkClient.exists("/efa", true);
        System.out.println(result.getVersion());
    }

    @Test
    public void delete() throws Exception {
        Stat stat = zkClient.exists("/efa", true);
        zkClient.delete("/efa", stat.getVersion());
    }

    public void registry() throws Exception {
        byte[] data = zkClient.getData("/a", e -> {
            try {
                registry();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }, null);
        System.out.println(new String(data));
    }

    @Test
    public void testRegistry() throws Exception{
        registry();
        Thread.sleep(Long.MAX_VALUE);
    }

}
