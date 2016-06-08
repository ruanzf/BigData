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

    public static void nioRead(){
        RandomAccessFile aFile = null;
        try{
            aFile = new RandomAccessFile("C:\\Users\\ruan\\Desktop\\nio.txt","rw");
            FileChannel fileChannel = aFile.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(1024);

            int bytesRead = fileChannel.read(buf);
            System.out.println(bytesRead);

            while(bytesRead != -1)
            {
                buf.flip();
                while(buf.hasRemaining())
                {
                    System.out.print((char)buf.get());
                }

                buf.compact();
                bytesRead = fileChannel.read(buf);
            }
            System.out.print("");
        }catch (IOException e){
            e.printStackTrace();
        }finally{
            try{
                if(aFile != null){
                    aFile.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public static void ioRead(){
        InputStream in = null;
        try{
            in = new BufferedInputStream(new FileInputStream("C:\\Users\\ruan\\Desktop\\nio.txt"));

            byte [] buf = new byte[1024];
            int bytesRead = in.read(buf);
            while(bytesRead != -1)
            {
                for(int i=0;i<bytesRead;i++)
                    System.out.print((char)buf[i]);
                bytesRead = in.read(buf);
            }
            System.out.println("");
        }catch (IOException e)
        {
            e.printStackTrace();
        }finally{
            try{
                if(in != null){
                    in.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
