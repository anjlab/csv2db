package com.anjlab.csv2db;

import java.util.concurrent.Callable;

public abstract class VoidCallable<E extends Exception> implements Callable<Void>
{
    public abstract void run() throws E;

    @Override
    public Void call() throws Exception
    {
        try
        {
            run();
        }
        catch (Exception e)
        {
            throw e;
        }
        return null;
    }
}