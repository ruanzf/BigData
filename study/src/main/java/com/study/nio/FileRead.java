package com.study.nio;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by ruan on 2016/5/23.
 */
public class FileRead {

    public static void main(String[] args) {
//        nioRead();
        ioRead();
    }

    public static void nioRead() {
        RandomAccessFile aFile = null;
        try {
            aFile = new RandomAccessFile("C:\\Users\\ruan\\Desktop\\nio.txt", "rw");
            FileChannel fileChannel = aFile.getChannel();   //创建通道
            ByteBuffer buf = ByteBuffer.allocate(1024);     //创建缓冲区
            int bytesRead = fileChannel.read(buf);          //将数据从通道读到缓冲区
            while (bytesRead != -1) {
                buf.flip();                                 //缓冲区设为读取模式 limit = position; position = 0;
                while (buf.hasRemaining()) {                 //还有东西读取 position < limit
                    System.out.print((char) buf.get());
                }
                buf.compact();                              //为写入数据清场子，把还没读完的数据挪到数组的开头，下回还可以读取到
                                                            //clear 是把数据全清除了
                bytesRead = fileChannel.read(buf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (aFile != null) {
                    aFile.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void ioRead() {
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream("C:\\Users\\ruan\\Desktop\\nio.txt"));

            byte[] buf = new byte[1024];
            int bytesRead = in.read(buf);
            while (bytesRead != -1) {
                for (int i = 0; i < bytesRead; i++)
                    System.out.print((char) buf[i]);
                bytesRead = in.read(buf);
            }
            System.out.println("");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
