package com.anjlab.csv2db;

import java.util.concurrent.Callable;

public abstract class VoidCallable<E extends Throwable> implements Callable<Void>
{
    public abstract void run() throws E;

    @Override
    public Void call() throws Exception
    {
        try
        {
            run();
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
        return null;
    }
}