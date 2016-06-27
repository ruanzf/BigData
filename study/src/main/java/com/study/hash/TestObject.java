package com.study.hash;

/**
 * Created by ruan on 2016/6/20.
 */
public class TestObject {

    private int i;

    public TestObject() {
        this.i = 1;
    }

    public void testSynchronized(String name) throws InterruptedException {
//        synchronized (TestObject.class) {
        synchronized (this) {
            System.out.println(name + "test start");
            Thread.sleep(10000);
            System.out.println(i);
            System.out.println(name + "test end");
        }
    }

}
