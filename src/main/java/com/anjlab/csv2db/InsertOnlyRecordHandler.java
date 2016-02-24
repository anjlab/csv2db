package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Class to insert missing values, totally ignoring rows where the primary key already exists
 * NB: depending on your data you might want to set batch size to 1 (or very low) to avoid
 * duplicates in this scenario
 * 
 * @author adam
 *
 */
public class InsertOnlyRecordHandler extends MergeRecordHandler
{

    public InsertOnlyRecordHandler(
            Configuration config,
            Connection connection,
            ScriptEngine scriptEngine,
            Router router,
            int threadId,
            int threadCount)
                    throws SQLException, ScriptException
    {
        super(config, connection, scriptEngine, router, threadId, threadCount);
    }

    @Override
    protected void performInsert(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        if (config.isIgnoreNullPK())
        {
            for (String primaryKeyColumnName : config.getPrimaryKeys())
            {
                if (transform(primaryKeyColumnName, nameValues) == null)
                {
                    // don't perform an insert if any of the PK values are null
                    return;
                }
            }
        }

        super.performInsert(nameValues);
    }

    @Override
    protected void performUpdate(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException
    {
        // Do nothing
    }
}
