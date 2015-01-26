package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.ResultSet;
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

    public InsertOnlyRecordHandler(Configuration config, Connection connection, ScriptEngine scriptEngine) throws SQLException,
            ScriptException
    {
        super(config, connection, scriptEngine);
    }

    @Override
    public void handleRecord(Map<String, String> nameValues) throws SQLException, ConfigurationException, ScriptException
    {
        selectStatement.clearParameters();

        int parameterIndex = 1;

        for (String primaryKeyColumnName : config.getPrimaryKeys())
        {
            selectStatement.setObject(parameterIndex++, transform(primaryKeyColumnName, nameValues));
        }

        ResultSet resultSet = selectStatement.executeQuery();

        try
        {
            if (resultSet.next())
            {
                return;
            }
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
            // Perform insert
            insertRecordHandler.handleRecord(nameValues);
        }
        finally
        {
            closeQuietly(resultSet);
        }
    }

}
