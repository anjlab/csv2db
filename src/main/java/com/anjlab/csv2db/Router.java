package com.anjlab.csv2db;

import java.util.Map;

public interface Router
{

    void dispatch(Map<String, Object> nameValues, int forThreadId) throws InterruptedException;

}
