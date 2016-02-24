package com.anjlab.csv2db;

public interface Mediator extends Router
{
    void dispatch(String[] line) throws InterruptedException;

    void producerDone() throws InterruptedException;

    Object take(int forThreadId) throws InterruptedException;

    void consumerDone(int threadId) throws InterruptedException;
}
