package com.study.jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ruan on 2016/6/21.
 * -XX:+PrintGCDetails -Xloggc:gc_log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=dump_log
 */
public class TestJvm {

    public static int _20M = 1024 * 1024 * 20;

    public static void main(String[] args) throws InterruptedException {
        List<BigObject> list = new ArrayList<BigObject>(100);
        int i = 0;
        while (true) {
            i++;
            new TestJvm().testList(i, list);
            System.out.println(list.size());

            new TestJvm().test();
            Thread.sleep(500);
        }
    }

    private void test() {
        System.out.println(new BigObject().bytes.length);
    }

    private void testList(int i, List<BigObject> list) {
        if((i % 100) == 0)
            list = new ArrayList<BigObject>(20);

        list.add(new BigObject());
        System.out.println(list.size());
    }

}

 class BigObject{
    public byte[] bytes = new byte[TestJvm._20M];
}