package com.study.hash;

/**
 * Created by ruan on 2016/6/20.
 */
public class TestEqual {

    public static void main(String[] args) {
        System.out.println((int)Math.pow(2, 31));
        testEqual();
    }

    private static void testSynchronized() {
        for (int i=0; i<3; i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        new TestObject().testSynchronized(Thread.currentThread().getName());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    private static void testEqual() {
        /**
         * 在没有重写hashCode  equals 时
         * hashcode 调用的是native方法，猜测是对象头 Mark Word 里面记录的hashcode
         * equals 调用的是object ==
         */
        TestObject obj1 = new TestObject();
        TestObject obj2 = new TestObject();
        System.out.println(obj1.hashCode());
        System.out.println(obj2.hashCode());
        System.out.println(obj1.equals(obj2));
    }
}
