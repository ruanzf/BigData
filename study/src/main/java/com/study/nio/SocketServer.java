package com.study.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by ruan on 2016/5/24.
 */
public class SocketServer {

    private static final int BUF_SIZE=1024;
    private static final int PORT = 1234;
    private static final int TIMEOUT = 3000;

    public static void main(String[] args)
    {
        selector();
    }

    public static void handleAccept(SelectionKey key) throws IOException{
        System.out.println("handleAccept");
        ServerSocketChannel ssChannel = (ServerSocketChannel)key.channel();
        SocketChannel sc = ssChannel.accept();
        sc.configureBlocking(false);
        sc.register(key.selector(), SelectionKey.OP_READ, ByteBuffer.allocateDirect(BUF_SIZE));
    }

    public static void handleRead(SelectionKey key){
        System.out.println("handleRead");
        try {
            SocketChannel sc = (SocketChannel)key.channel();
            ByteBuffer buf = (ByteBuffer)key.attachment();
            long bytesRead = sc.read(buf);
            buf.put("copy".getBytes());
            sc.write(buf);
            buf.clear();
            System.out.println("handleRead" + bytesRead);
            while(bytesRead>0){
                buf.flip();
                while(buf.hasRemaining()){
                    System.out.print((char)buf.get());
                }
                System.out.println();
                buf.clear();
                bytesRead = sc.read(buf);
            }

            key.interestOps(SelectionKey.OP_READ);

            buf.put("copy".getBytes());
            sc.write(buf);
            buf.clear();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void handleWrite(SelectionKey key) throws IOException{
        System.out.println("handleWrite");
        ByteBuffer buf = (ByteBuffer)key.attachment();
        buf.flip();
        SocketChannel sc = (SocketChannel) key.channel();
        while(buf.hasRemaining()){
            sc.write(buf);
        }
        buf.compact();
    }

    public static void selector() {
        Selector selector = null;
        ServerSocketChannel ssc = null;
        try{
            selector = Selector.open();
            ssc= ServerSocketChannel.open();
            ssc.socket().bind(new InetSocketAddress(PORT));
            ssc.configureBlocking(false);
            ssc.register(selector, SelectionKey.OP_ACCEPT);

            while(true){
                int s = selector.select(TIMEOUT);
                System.out.println("==" + s);
                if(s == 0){
                    continue;
                }
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while(iter.hasNext()){
                    SelectionKey key = iter.next();
                    if(key.isAcceptable()){
                        handleAccept(key);
                    }
                    if(key.isReadable()){
                        handleRead(key);
                    }
                    if(key.isValid() && key.isWritable()){
                        handleWrite(key);
                    }
                    if(key.isConnectable()){
                        System.out.println("isConnectable = true");
                    }
                    iter.remove();
                }
            }

        }catch(IOException e){
            e.printStackTrace();
        }finally{
            try{
                if(selector!=null){
                    selector.close();
                }
                if(ssc!=null){
                    ssc.close();
                }
            }catch(IOException e){
                e.printStackTrace();
            }
        }
    }
}