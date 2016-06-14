package com.zk.normal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ruan on 2016/6/12.
 * curator 有两套监听机制
 * 一个是封装了zk自身的watcher
 * watcher 封装了zk原本的watcher 可以跨进程使用, 但是无法在inbackground的情况下触发watcher
 * 一个是自己的listener
 * listener 只能监听相同thread的client事件, 跨thread或者跨process则不行, 操作必须使用inbackground()模式才能触发listener
 */
public class MonitorExample {

    public static CuratorFramework client;
    public static CuratorWatcher watcher;
    public static BackgroundCallback callback;

    /**
     * Zookeeper提供了几种认证方式
     * 1、world：有个单一的ID，anyone，表示任何人
     * 2、auth：不使用任何ID，表示任何通过验证的用户
     * 3、digest：使用 用户名：密码 字符串生成MD5哈希值作为ACL标识符ID
     * 4、ip：使用客户端主机ip地址作为一个ACL标识符
     */
    public static List<ACL> acls = new ArrayList<>(1);


    static {
        try {
            acls.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest("ruanzf:rzf123"))));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
        client.setACL().withACL(acls);


    }

    public static void main(String[] args) throws Exception {
        watcher = watcher();
        callback = callback();

        createUseListener();

//        create();
//        update();
//        delete();
    }

    private static CuratorWatcher watcher() {

        //lambda 表达式
        return event -> {
            switch (event.getType()) {
                case None: {
                    System.out.println("none, do nothing");
                    break;
                }
                case NodeCreated: {
                    System.out.println(String.format("Node created: %s ", event.getPath()));
                    break;
                }
                case NodeDeleted: {
                    System.out.println(String.format("Node deleted: %s ", event.getPath()));
                    break;
                }
                case NodeDataChanged: {
                    System.out.println(String.format("Node data changed: %s ", event.getPath()));
                    break;
                }
                case NodeChildrenChanged: {
                    System.out.println(String.format("Node children changed: %s ", event.getPath()));
                    break;
                }
            }
        };
    }

    private static void create() throws Exception {
        if (null == client.checkExists().usingWatcher(watcher).forPath("/ruanzf/watcher")) {
            client.create().forPath("/ruanzf/watcher", "watcher".getBytes());
        }

        if (null == client.checkExists().forPath("/ruanzf/watcher/child1")) {
            client.getChildren().usingWatcher(watcher).forPath("/ruanzf/watcher");
            client.create().forPath("/ruanzf/watcher/child1", "child1".getBytes());
        }
    }

    private static void update() throws Exception {
        client.getData().usingWatcher(watcher).forPath("/ruanzf/watcher");
        client.setData().forPath("/ruanzf/watcher", "new_watcher".getBytes());
    }

    private static void delete() throws Exception {
        client.checkExists().usingWatcher(watcher).forPath("/ruanzf/watcher");
        client.delete().forPath("/ruanzf/watcher/child1");
        client.delete().forPath("/ruanzf/watcher");
    }

    private static BackgroundCallback callback() {
        return (client, event) -> {
            switch (event.getType()) {
                case CREATE: {
                    System.out.println(String.format("Node created: %s", event.getPath()));
                    break;
                }
                case DELETE: {
                    System.out.println(String.format("Node deleted: %s", event.getPath()));
                    break;
                }
                case EXISTS: {
                    System.out.println(String.format("Node exists: %s", event.getPath()));
                    break;
                }
                case SET_DATA: {
                    System.out.println(String.format("Node set data: %s", event.getPath()));
                    break;
                }
                case GET_DATA: {
                    System.out.println(String.format("Node get data: %s", new String(event.getData())));
                    break;
                }
                case SET_ACL: {
                    System.out.println(String.format("Node set acl: %s", event.getPath()));
                    break;
                }
                case GET_ACL: {
                    System.out.println(String.format("Node get acl: %s, acl: %s", event.getPath(), event.getACLList().get(0).toString()));
                    break;
                }
            }
        };
    }

    private static void createUseListener() throws Exception {
        if (null == client.checkExists().inBackground(callback).forPath("/ruanzf/callback")) {
            client.create().inBackground(callback).forPath("/ruanzf/callback", "callback".getBytes());
        }

        client.getData().inBackground(callback).forPath("/ruanzf/callback");

        client.setData().inBackground(callback).forPath("/ruanzf/callback", "new callback".getBytes());

        client.setACL().withACL(acls).inBackground(callback).forPath("/ruanzf/callback");

        client.getACL().inBackground(callback).forPath("/ruanzf/callback");

        client.delete().inBackground(callback).forPath("/ruanzf/callback");

        Thread.sleep(1000);
    }
}
