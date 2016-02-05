package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class InsertRecordHandler extends AbstractRecordHandler
{
    private PreparedStatement insertStatement;

    private int numberOfStatementsInBatch;

    public InsertRecordHandler(Configuration config, Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
    {
        super(config, scriptEngine, connection);

        StringBuilder insertClause =
                new StringBuilder("INSERT INTO ")
                        .append(config.getTargetTable())
                        .append(" (");

        StringBuilder valuesClause = new StringBuilder();

        for (String targetTableColumnName : getColumnNamesWithInsertValues())
        {
            if (valuesClause.length() > 0)
            {
                insertClause.append(", ");
                valuesClause.append(", ");
            }
            insertClause.append(targetTableColumnName);

            ValueDefinition definition = config.getInsertValues().get(targetTableColumnName);

            if (definition.producesSQL())
            {
                valuesClause.append(definition.eval(targetTableColumnName, null, scriptEngine));
            }
            else
            {
                valuesClause.append("?");
            }
        }

        for (String targetTableColumnName : getOrderedTableColumnNames())
        {
            if (valuesClause.length() > 0)
            {
                insertClause.append(", ");
                valuesClause.append(", ");
            }
            insertClause.append(targetTableColumnName);
            valuesClause.append("?");
        }

        insertClause.append(") VALUES (")
                .append(valuesClause)
                .append(")");

        if (Import.isVerboseEnabled())
        {
            Import.logVerbose("INSERT statement used: " + insertClause);
        }

        insertStatement = connection.prepareStatement(insertClause.toString());
    }

    @Override
    public void handleRecord(Map<String, Object> nameValues) throws SQLException, ConfigurationException, ScriptException
    {
        int parameterIndex = 1;

        if (Import.isVerboseEnabled())
        {
            printNameValues(nameValues);
        }

        for (String targetTableColumnName : getColumnNamesWithInsertValues())
        {
            ValueDefinition definition = config.getInsertValues().get(targetTableColumnName);

            if (!definition.producesSQL())
            {
                Object columnValue = eval(definition, targetTableColumnName, nameValues);

                if (Import.isVerboseEnabled())
                {
                    printNameValue(targetTableColumnName, columnValue);
                }

                insertStatement.setObject(parameterIndex++, columnValue);
            }
        }

        for (String targetTableColumnName : getOrderedTableColumnNames())
        {
            Object columnValue = transform(targetTableColumnName, nameValues);

            if (Import.isVerboseEnabled())
            {
                printNameValue(targetTableColumnName, columnValue);
            }

            insertStatement.setObject(parameterIndex++, columnValue);
        }

        numberOfStatementsInBatch++;

        insertStatement.addBatch();

        checkBatchExecution(config.getBatchSize());
    }

    private void checkBatchExecution(int limit) throws SQLException
    {
        if (numberOfStatementsInBatch >= limit)
        {
            insertStatement.executeBatch();

            insertStatement.clearParameters();

            numberOfStatementsInBatch = 0;
        }
    }

    @Override
    public void close()
    {
        try
        {
            checkBatchExecution(0);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(insertStatement);
            super.close();
        }
    }
}
