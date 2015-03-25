package com.anjlab.csv2db;

import javax.script.ScriptException;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Map;

public interface RecordHandler extends Closeable
{
    void handleRecord(Map<String, Object> nameValues) throws SQLException, ConfigurationException, ScriptException;
}
