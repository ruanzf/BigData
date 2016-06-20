package com.study.exception;

/**
 * Created by ruan on 2016/6/15.
 */
public class ExceptionTest {

    private void throwNullPointerException(Integer i) {
        if (null == i)
            throw new NullPointerException("i is null");

        System.out.println(i);
    }

    public static void main(String[] args) {
        new ExceptionTest().throwNullPointerException(null);
        System.out.println("111");
    }
}
