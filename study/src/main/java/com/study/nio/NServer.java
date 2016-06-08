package com.study.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;


public class NServer {

    private Selector selector = null;
    static final int port = 30001;
    private Charset charset = Charset.forName("UTF-8");
    public void init() throws IOException
    {
        selector = Selector.open();
        ServerSocketChannel server = ServerSocketChannel.open();
        ServerSocket serverSocket = server.socket();
        serverSocket.bind(new InetSocketAddress(port));
        //非阻塞的方式
        server.configureBlocking(false);
        //注册到选择器上，设置为监听状态
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Server is listening now...");

        //此处阻塞监听
        while(selector.select() > 0)
        {

            for(SelectionKey sk : selector.selectedKeys())
            {
                selector.selectedKeys().remove(sk);
                //处理来自客户端的连接请求
                if(sk.isAcceptable())
                {
                    SocketChannel sc = server.accept();
                    //非阻塞模式
                    sc.configureBlocking(false);
                    //注册选择器，并设置为读取模式
                    sc.register(selector, SelectionKey.OP_READ);
                    //将此对应的channel设置为准备接受其他客户端请求
                    sk.interestOps(SelectionKey.OP_ACCEPT);
                    System.out.println("Server is listening from client :" + sc.socket().getRemoteSocketAddress());
                    sc.write(charset.encode("hi Im server"));
                }
                //处理来自客户端的数据读取请求
                if(sk.isReadable())
                {
                    //返回该SelectionKey对应的 Channel，其中有数据需要读取
                    SocketChannel sc = (SocketChannel)sk.channel();
                    ByteBuffer buff = ByteBuffer.allocate(1024);
                    StringBuilder content = new StringBuilder();
                    try
                    {
                        while(sc.read(buff) > 0)
                        {
                            buff.flip();
                            content.append(charset.decode(buff));

                        }
                        System.out.println("Server is listening from client " + sc.socket().getRemoteSocketAddress() + " data rev is: " + content);
                        //将此对应的channel设置为准备下一次接受数据
                        sk.interestOps(SelectionKey.OP_READ);
                    }
                    catch (IOException io)
                    {
                        sk.cancel();
                        if(sk.channel() != null)
                        {
                            sk.channel().close();
                        }
                    }
                    if(content.length() > 0)
                    {
                        //广播数据到所有的SocketChannel中
//                        for(SelectionKey key : selector.keys())
//                        {
//                            Channel targetchannel = key.channel();
//                            if(targetchannel instanceof SocketChannel)
//                            {
//                                SocketChannel dest = (SocketChannel)targetchannel;
//                                dest.write(charset.encode(content.toString()));
//                            }
//                        }

                        Channel targetchannel = sk.channel();
                        if(targetchannel instanceof SocketChannel)
                        {
                            SocketChannel dest = (SocketChannel)targetchannel;
                            sc.write(charset.encode(content.toString() + " copy"));
                        }
                    }

                }
            }
        }
    }

    public static void main(String[] args) throws IOException
    {
        new NServer().init();
    }
}