package com.study.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by ruan on 2016/5/23.
 */
public class SocketClient {

    private static Charset charset = Charset.forName("UTF-8");

    public static void main(String[] args) {
        new Thread(new Runnable() {
            public void run() {
                server();
            }
        })
//                .start()
        ;
        new Thread(new Runnable() {
            public void run() {
                client();
            }
        })
//                .start()
        ;
        new Thread(new Runnable() {
            public void run() {
                readWriteClient();
            }
        })
                .start()
        ;
    }

    public static void client() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        SocketChannel socketChannel = null;
        try {
            TimeUnit.SECONDS.sleep(1);
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            socketChannel.connect(new InetSocketAddress("127.0.0.1", 8080));

            if (socketChannel.finishConnect()) {
                int i = 0;
                while (true) {
                    TimeUnit.SECONDS.sleep(5);
                    String info = "I'm " + i++ + "-th information from client";
                    buffer.clear();
                    buffer.put(info.getBytes());
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        System.out.println(buffer);
                        socketChannel.write(buffer);
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (socketChannel != null) {
                    socketChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void readWriteClient() {
        Selector selector = null;
        boolean first = true;
        SocketChannel socketChannel = null;
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            TimeUnit.SECONDS.sleep(1);
            selector = Selector.open();
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress("127.0.0.1", 1234));
            socketChannel.register(selector, SelectionKey.OP_READ);

            if (socketChannel.finishConnect()) {
                while (true) {
                    int s = selector.select(1000);
                    if (s == 0) {
                        System.out.println("==" + s);
                        continue;
                    }

                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        if (key.isReadable()) {
                            SocketChannel sc = (SocketChannel) key.channel();
                            long bytesRead = sc.read(buffer);
                            System.out.println("handleRead" + bytesRead);
                            while (bytesRead > 0) {
                                buffer.flip();
                                while(buffer.hasRemaining()){
                                    System.err.print((char)buffer.get());
                                }
                                System.out.println();
                                buffer.clear();
                                bytesRead = sc.read(buffer);
                            }

                            key.interestOps(SelectionKey.OP_READ);
                            sc.write(charset.encode("hi im client"));
                            Thread.sleep(1000);
                        }

                        if (key.isValid() && key.isConnectable()) {
                            System.out.println("isConnectable = true");
                        }
                        iter.remove();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (socketChannel != null) {
                    socketChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void server() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        ServerSocket serverSocket = null;
        ServerSocketChannel serverSocketChannel = null;
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocket = serverSocketChannel.socket();
            serverSocket.bind(new InetSocketAddress(8080));
            serverSocketChannel.configureBlocking(true);
            while (true) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                if (null == socketChannel) {
                    System.out.println("continue");
                    continue;
                }
                System.err.println("Handling client at ");
                int bytesRead = socketChannel.read(buf);
                while (bytesRead != -1) {
                    buf.flip();
                    System.err.println(new String(buf.array()));
                    buf.clear();
                    bytesRead = socketChannel.read(buf);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
                if (serverSocketChannel != null) {
                    serverSocketChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
