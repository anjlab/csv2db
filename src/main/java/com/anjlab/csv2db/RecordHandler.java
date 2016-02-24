package com.anjlab.csv2db;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Map;

import javax.script.ScriptException;

public interface RecordHandler extends Closeable
{
    void handleRecord(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException;
}
