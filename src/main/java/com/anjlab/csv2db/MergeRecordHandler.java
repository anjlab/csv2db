package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.codahale.metrics.Timer;

public class MergeRecordHandler extends AbstractInsertUpdateRecordHandler
{
    protected AbstractRecordHandler insertRecordHandler;

    protected PreparedStatement updateStatement;

    private final Timer updateStatementTimer;

    private int numberOfStatementsInBatch;

    public MergeRecordHandler(
            Configuration config,
            Connection connection,
            ScriptEngine scriptEngine,
            Router router,
            int threadId,
            int threadCount)
                    throws SQLException, ScriptException
    {
        super(config, scriptEngine, connection, router, threadId, threadCount);

        StringBuilder setClause = new StringBuilder();

        for (String targetTableColumnName : getColumnNamesWithUpdateValues())
        {
            if (setClause.length() > 0)
            {
                setClause.append(", ");
            }
            setClause.append(targetTableColumnName).append(" = ");

            ValueDefinition definition = config.getUpdateValues().get(targetTableColumnName);

            if (definition.producesSQL())
            {
                setClause.append(definition.eval(targetTableColumnName, null, scriptEngine));
            }
            else
            {
                setClause.append("?");
            }
        }

        for (String targetTableColumnName : getOrderedTableColumnNames())
        {
            if (setClause.length() > 0)
            {
                setClause.append(", ");
            }
            setClause.append(targetTableColumnName).append(" = ?");
        }

        StringBuilder updateClause =
                new StringBuilder("UPDATE ")
                        .append(config.getTargetTable())
                        .append(" SET ")
                        .append(setClause)
                        .append(" WHERE ")
                        .append(buildWhereClause());

        if (Import.isVerboseEnabled())
        {
            Import.logVerbose("UPDATE statement used: " + updateClause);
        }

        this.updateStatement = connection.prepareStatement(updateClause.toString());

        this.insertRecordHandler = new InsertRecordHandler(
                config, connection, scriptEngine, router, threadId, threadCount);

        this.updateStatementTimer = Import.METRIC_REGISTRY.timer("thread-" + threadId + ".updates");
    }

    @Override
    protected void enableBatchExecution() throws SQLException
    {
        super.enableBatchExecution();

        checkBatchExecution(config.getBatchSize());

        insertRecordHandler.enableBatchExecution();
    }

    @Override
    protected void disableBatchExecution()
    {
        super.disableBatchExecution();

        insertRecordHandler.disableBatchExecution();
    }

    @Override
    protected void performInsert(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        insertRecordHandler.handleRecord(nameValues);
    }

    @Override
    protected void performUpdate(Map<String, Object> nameValues)
            throws ScriptException, SQLException, ConfigurationException
    {
        int parameterIndex = 1;

        //  Set parameters for the SET clause
        for (String targetTableColumnName : getColumnNamesWithUpdateValues())
        {
            ValueDefinition definition = config.getUpdateValues().get(targetTableColumnName);

            if (!definition.producesSQL())
            {
                Object columnValue = eval(definition, targetTableColumnName, nameValues);

                if (Import.isVerboseEnabled())
                {
                    printNameValue(targetTableColumnName, columnValue);
                }

                updateStatement.setObject(parameterIndex++, columnValue);
            }
        }

        for (String targetTableColumnName : getOrderedTableColumnNames())
        {
            Object columnValue = transform(targetTableColumnName, nameValues);

            if (Import.isVerboseEnabled())
            {
                printNameValue(targetTableColumnName, columnValue);
            }

            updateStatement.setObject(parameterIndex++, columnValue);
        }

        //  Set parameters for the WHERE clause
        for (String primaryKeyColumnName : config.getPrimaryKeys())
        {
            Object primaryKeyColumnValue = transform(primaryKeyColumnName, nameValues);

            if (Import.isVerboseEnabled())
            {
                printNameValue(primaryKeyColumnName, primaryKeyColumnValue);
            }

            updateStatement.setObject(parameterIndex++, primaryKeyColumnValue);
        }

        numberOfStatementsInBatch++;

        updateStatement.addBatch();

        checkBatchExecution(config.getBatchSize());
    }

    private void checkBatchExecution(int limit) throws SQLException
    {
        if (batchExecutionDisabled)
        {
            return;
        }

        if (numberOfStatementsInBatch >= limit)
        {
            Import.measureTime(updateStatementTimer, new VoidCallable<SQLException>()
            {
                @Override
                public void run() throws SQLException
                {
                    updateStatement.executeBatch();
                }
            });

            updateStatement.clearParameters();

            numberOfStatementsInBatch = 0;
        }
    }

    @Override
    public void close()
    {
        try
        {
            super.executeBatch();

            checkBatchExecution(0);

            insertRecordHandler.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(updateStatement);
            super.close();
        }
    }
}