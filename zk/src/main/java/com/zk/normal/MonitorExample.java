package com.zk.normal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * Created by ruan on 2016/6/12.
 */
public class MonitorExample {

    public static CuratorFramework client;

    static {
        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
        client = client.usingNamespace("ruanzf");
    }

    public static void main(String[] args) {

    }

    private void watch() throws Exception {
    }
}
