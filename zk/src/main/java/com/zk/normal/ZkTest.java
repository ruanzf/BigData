package com.zk.normal;

import com.google.common.primitives.Bytes;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * Created by ruan on 2016/6/7.
 */
public class ZkTest {

    public static CuratorFramework client;

    static {
//        client = CuratorFrameworkFactory.builder().
//                namespace("ruanzf").
//                connectString("192.168.1.118:2181").
//                sessionTimeoutMs(60 * 1000).
//                connectionTimeoutMs(15 * 1000).
//                retryPolicy(new RetryNTimes(3, 1000)).
//                build();

        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
        client = client.usingNamespace("ruanzf");
    }

    public static void main(String[] args) throws Exception {
        create();
    }

    public static void create() throws Exception {

        /**
         * null means that it does not exist
         */
        if(null == client.checkExists().forPath("/one/1")) {
            client.create().creatingParentsIfNeeded().forPath("/one/1");
        }else {
            System.out.println("exists");
        }
    }


}
