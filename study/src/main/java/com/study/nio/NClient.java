package com.study.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Scanner;


public class NClient {

    private Selector selector = null;
    static final int port = 30001;
    private Charset charset = Charset.forName("UTF-8");
    private SocketChannel sc = null;

    public void init() throws IOException {
        selector = Selector.open();
        sc = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
        sc.configureBlocking(false);
        sc.register(selector, SelectionKey.OP_READ);
        new Thread(new ClientThread()).start();
        Scanner scan = new Scanner(System.in);          //在主线程中 从键盘读取数据输入到服务器端
        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            sc.write(charset.encode(line));
        }
    }

    private class ClientThread implements Runnable {
        public void run() {
            try {
                while (selector.select() > 0) {
                    for (SelectionKey sk : selector.selectedKeys()) {
                        selector.selectedKeys().remove(sk);
                        if (sk.isReadable()) {
                            //使用 NIO 读取 Channel中的数据
                            SocketChannel sc = (SocketChannel) sk.channel();
                            ByteBuffer buff = ByteBuffer.allocate(1024);
                            String content = "";
                            while (sc.read(buff) > 0) {
                                buff.flip();
                                content += charset.decode(buff);
                            }
                            System.out.println("聊天信息： " + content);
                            sk.interestOps(SelectionKey.OP_READ);
                        }
                    }
                }
            } catch (IOException io) {
            }
        }
    }


    public static void main(String[] args) throws IOException {
        new NClient().init();
    }
}