package com.zk.normal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

/**
 * Created by ruan on 2016/6/7.
 */
public class ZkCRUDExample {

    public static CuratorFramework client;

    static {
        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
        client = client.usingNamespace("ruanzf");
    }

    public static void main(String[] args) throws Exception {
//        create();
        update();
//        delete();
    }

    public static void create() throws Exception {
        /**
         * null means that it does not exist
         */
        if(null == client.checkExists().forPath("/two")) {

            /**
             * 1 create
            client.create().creatingParentsIfNeeded().forPath("/two");
             */

            /**
             * 2 create
             * EPHEMERAL_SEQUENTIAL
             * 请点进去看源码注释解释
             */
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/two");

            /**
             * 3 create
             * Protection Mode:
             * 请点进去看源码注释解释
            client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL).forPath("/two");
             */

        }else {
            System.out.println("exists");
        }
    }

    public static void update() throws Exception {
        if(null == client.checkExists().forPath("/two")) {
            System.out.println("not exists");
        }else {
            System.out.println(String.format("before update, state:%s, data:%s.", client.getState().toString(), new String(client.getData().forPath("/two")), "UTF-8"));

            /**
             * 1 set data
             */
            client.setData().forPath("/two", "this is two".getBytes());


            /**
             * 2 set data
             * asynchronously，via callback
             * getCuratorListenable() is only available from a non-namespaced CuratorFramework instance

            CuratorListener listener = new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    System.out.println("this is callback");
                }
            };
            client.getCuratorListenable().addListener(listener);
            client.setData().inBackground().forPath("/two", "this is two".getBytes());
             */

            /**
             * 3 set data
             * asynchronously，via callback
            client.setData().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    System.out.println("this is callback");
                }
            }).forPath("/two", "two2".getBytes());
             */

            Thread.sleep(1000);
            System.out.println(String.format("after update, state:%s, data:%s.", client.getState().toString(), new String(client.getData().forPath("/two")), "UTF-8"));
        }
    }

    public static void delete() throws Exception {
        if(null == client.checkExists().forPath("/three")) {
            System.out.println("not exists, so I create");
            client.create().forPath("/three", "this is three".getBytes());
            System.out.println(String.format("after create, data:%s, and I delete it", new String(client.getData().forPath("/three")), "UTF-8"));
            client.delete().forPath("/three");
            System.out.println(String.format("so, result is: %s", null == client.checkExists().forPath("/three")));
        }
    }
}
