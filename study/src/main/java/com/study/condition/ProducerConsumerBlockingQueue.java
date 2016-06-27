package com.study.condition;


import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by ruan on 2016/3/24.
 * ArrayBlockingQueue ：     一个由数组结构组成的有界阻塞队列。
 * LinkedBlockingQueue ：    一个由链表结构组成的有界阻塞队列。
 * PriorityBlockingQueue ：  一个支持优先级排序的无界阻塞队列。
 * DelayQueue：              一个使用优先级队列实现的无界阻塞队列。
 * SynchronousQueue：        一个不存储元素的阻塞队列。
 * LinkedTransferQueue：     一个由链表结构组成的无界阻塞队列。
 * LinkedBlockingDeque：     一个由链表结构组成的双向阻塞队列
 */
public class ProducerConsumerBlockingQueue {

    /**
     * private final Condition notEmpty;    取个别名叫不能空
     * private final Condition notFull;     取个别名叫不能满
     * 1、添加时，如果满了，则“不能满”你先等着（notFull.await()）
     *      否则往数组里加元素，并告诉“不能空”你可以干活了（notEmpty.signal();）
     *
     * 2、取出时，如果空了，则“不能空”你先等着（notEmpty.await()）
     *      否则往数组里取元素，并告诉“不能满”你可以干活了（notFull.signal();）
     */
    private ArrayBlockingQueue blockingQueue = new ArrayBlockingQueue(10);

    class Consumer extends Thread{
        @Override
        public void run() {
            while (true) {

                System.out.println("Im consumer");
                try {
                    blockingQueue.take();
                    System.out.println("从队列取走一个元素，队列剩余" + blockingQueue.size());
                    Thread.sleep(sleepTime());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class Producer extends Thread{
        @Override
        public void run() {
            while (true) {
                System.out.println("Im producer");

                try {
                    blockingQueue.put(1);
                    System.out.println("往队列添加一个元素，队列大小" + blockingQueue.size());
                    Thread.sleep(sleepTime());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        ProducerConsumerBlockingQueue test = new ProducerConsumerBlockingQueue();
        test.new Producer().start();
        test.new Consumer().start();
    }

    private int sleepTime() {
        return (int)(Math.random() * 1000);
    }
}
