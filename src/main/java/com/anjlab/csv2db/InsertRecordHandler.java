package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.codahale.metrics.Timer;

public class InsertRecordHandler extends AbstractRecordHandler
{
    private PreparedStatement insertStatement;

    private final Timer insertStatementTimer;

    private int numberOfStatementsInBatch;

    private final Set<String> duplicatesTracker;

    public InsertRecordHandler(
            Configuration config,
            Connection connection,
            ScriptEngine scriptEngine,
            Router router,
            int threadId,
            int threadCount)
                    throws SQLException, ScriptException
    {
        super(config, scriptEngine, connection, router, threadId, threadCount);

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

        insertStatementTimer = Import.METRIC_REGISTRY.timer("thread-" + threadId + ".inserts");

        duplicatesTracker = new HashSet<>(config.getBatchSize());
    }

    @Override
    public void handleRecord(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        if (Import.isVerboseEnabled())
        {
            printNameValues(nameValues);
        }

        // XXX Check duplicates should be performed on eval'ed/transformed values,
        // right now it's partially true (i.e. eval'ed but not transformed values are used),
        // and only if map function is declared in configuration
        if (config.isIgnoreDuplicatePK())
        {
            // If needed re-route this to another handler based on keys hash
            String keys = config.joinPrimaryKeys(nameValues);

            int partitionId = Math.abs(keys.hashCode() % threadCount);

            if (partitionId != threadId)
            {
                router.dispatch(nameValues, partitionId);
                return;
            }

            if (duplicatesTracker.contains(keys))
            {
                // This record will be ignored
                if (Import.isVerboseEnabled())
                {
                    Import.logVerbose("Duplicate already in batch for keys: " + keys);
                }
                return;
            }

            duplicatesTracker.add(keys);
        }

        int parameterIndex = 1;

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

    @Override
    protected void enableBatchExecution() throws SQLException
    {
        super.enableBatchExecution();

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
            if (Import.isVerboseEnabled())
            {
                Import.logVerbose("About to flush INSERT batch");
            }

            Import.measureTime(insertStatementTimer, new VoidCallable<SQLException>()
            {
                @Override
                public void run() throws SQLException
                {
                    insertStatement.executeBatch();
                }
            });

            insertStatement.clearParameters();

            numberOfStatementsInBatch = 0;

            duplicatesTracker.clear();
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
