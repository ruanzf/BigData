package com.zk.normal;

import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Created by ruan on 2016/6/12.
 */
public class PathCacheExample {

    private static CuratorFramework client;
    private static final String PATH = "/cache_test";

    static {
        client = CuratorFrameworkFactory.newClient("192.168.1.118:2181", new RetryNTimes(3, 1000));
        client.start();
        client = client.usingNamespace("ruanzf");
    }

    public static void main(String[] args) throws Exception {
        PathChildrenCache cache = null;
        try {
            // in this example we will cache data. Notice that this is optional.
            cache = new PathChildrenCache(client, PATH, true);
            cache.start();

            processCommands(client, cache);
            client.close();
        } finally {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    private static void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println(String.format("Node added: %s : %s ", event.getData().getPath(), new String(event.getData().getData(), "UTF-8")));
                        break;
                    }

                    case CHILD_UPDATED: {
                        System.out.println(String.format("Node changed: %s : %s ", event.getData().getPath(), new String(event.getData().getData(), "UTF-8")));
                        break;
                    }

                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + event.getData().getPath());
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private static void processCommands(CuratorFramework client, PathChildrenCache cache) throws Exception {
        printHelp();
        addListener(cache);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done) {
            System.out.print("> ");
            String line = in.readLine();
            if (line == null) {
                break;
            }

            String command = line.trim();
            String[] parts = command.split("\\s");
            if (parts.length == 0) {
                continue;
            }
            String operation = parts[0];
            String args[] = Arrays.copyOfRange(parts, 1, parts.length);

            if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?")) {
                printHelp();
            } else if (operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit")) {
                done = true;
            } else if (operation.equals("set")) {
                setValue(client, command, args);
            } else if (operation.equals("remove")) {
                remove(client, command, args);
            } else if (operation.equals("list")) {
                list(cache);
            }

            Thread.sleep(1000); // just to allow the console output to catch up
        }
    }

    private static void list(PathChildrenCache cache) {
        if (0 == cache.getCurrentData().size()) {
            System.out.println("* empty *");
        } else {
            for (ChildData data : cache.getCurrentData()) {
                System.out.println(data.getPath() + " = " + new String(data.getData()));
            }
        }
    }

    private static void remove(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("syntax error (expected remove <path>): " + command);
            return;
        }

        String name = args[0];
        if (name.contains("/")) {
            System.err.println("Invalid node name" + name);
            return;
        }

        String path = ZKPaths.makePath(PATH, name);
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            // ignore
        }
    }

    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("syntax error (expected set <path> <value>): " + command);
            return;
        }

        String name = args[0];
        if (name.contains("/")) {
            System.err.println("Invalid node name" + name);
            return;
        }
        String path = ZKPaths.makePath(PATH, name);
        byte[] bytes = args[1].getBytes();
        try {
            client.setData().forPath(path, bytes);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("create");
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
        }
    }

    private static void printHelp() {
        System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
        System.out.println("set <name> <value>: Adds or updates a node with the given name");
        System.out.println("remove <name>: Deletes the node with the given name");
        System.out.println("list: List the nodes/values in the cache");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}
