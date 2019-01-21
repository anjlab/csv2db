package com.anjlab.csv2db;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

public abstract class AbstractInsertUpdateRecordHandler extends AbstractRecordHandler
{
    private Map<Integer, PreparedStatement> selectStatements;

    private final Timer selectStatementTimer;

    private StringBuilder whereClause;

    private final List<Pair<String, Map<String, Object>>> nameValuesBuffer;

    public AbstractInsertUpdateRecordHandler(
            Configuration config,
            ScriptEngine scriptEngine,
            Connection connection,
            Router router,
            int threadId,
            int threadCount)
                    throws SQLException
    {
        super(config, scriptEngine, connection, router, threadId, threadCount);

        if (config.getPrimaryKeys() == null || config.getPrimaryKeys().isEmpty())
        {
            throw new RuntimeException("primaryKeys required for " + config.getOperationMode() + " mode");
        }

        nameValuesBuffer = new ArrayList<>(config.getBatchSize());

        selectStatements = new HashMap<>();

        selectStatementTimer = Import.METRIC_REGISTRY.timer("thread-" + threadId + ".selects");
    }

    private PreparedStatement getOrCreateSelectStatement(int batchSize) throws SQLException
    {
        if (batchSize < 1)
        {
            throw new IllegalArgumentException("batchSize < 1");
        }

        PreparedStatement statement = selectStatements.get(batchSize);

        if (statement == null)
        {
            StringBuilder selectClause =
                    new StringBuilder("SELECT ")
                            .append(StringUtils.join(config.escapeSqlNames(getOrderedTableColumnNames()), ", "))
                            .append(" FROM ")
                            .append(config.escapeSqlName(config.getTargetTable()))
                            .append(" WHERE ")
                            .append(buildWhereClause());

            for (int i = 1; i < batchSize; i++)
            {
                selectClause.append(" OR (")
                        .append(buildWhereClause())
                        .append(")");
            }

            statement = connection.prepareStatement(selectClause.toString());

            if (Import.isVerboseEnabled())
            {
                Import.logVerbose("SELECT statement used: " + selectClause);
            }

            selectStatements.put(batchSize, statement);
        }

        return statement;
    }

    protected StringBuilder buildWhereClause()
    {
        if (whereClause == null)
        {
            whereClause = new StringBuilder();

            for (String columnName : config.getPrimaryKeys())
            {
                if (whereClause.length() > 0)
                {
                    whereClause.append(" AND ");
                }

                whereClause.append(config.escapeSqlName(columnName)).append(" = ?");
            }
        }

        return whereClause;
    }

    @Override
    public void handleRecord(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        if (Import.isVerboseEnabled())
        {
            printNameValues(nameValues);
        }

        if (addBatch(nameValues))
        {
            return;
        }

        executeBatch();
    }

    protected void executeBatch() throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        if (nameValuesBuffer.isEmpty())
        {
            return;
        }

        try (ResultSet resultSet = selectBatch())
        {
            disableBatchExecution();

            handleRecordsBatch(toPrimaryKeysHashMap(resultSet));
        }
        finally
        {
            nameValuesBuffer.clear();

            enableBatchExecution();
        }
    }

    private void handleRecordsBatch(Map<String, Map<String, Object>> primaryKeysHashMap)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException
    {
        for (Pair<String, Map<String, Object>> pair : nameValuesBuffer)
        {
            Map<String, Object> nameValues = pair.getValue();

            Map<String, Object> parsedResultSet = primaryKeysHashMap.get(pair.getKey());

            if (parsedResultSet != null)
            {
                if (!config.isForceUpdate())
                {
                    boolean dataChanged = false;

                    for (String targetTableColumnName : getOrderedTableColumnNames())
                    {
                        Object oldValue = parsedResultSet.get(targetTableColumnName);
                        Object newValue = transform(targetTableColumnName, nameValues);

                        if (Import.isVerboseEnabled())
                        {
                            printNameValue(targetTableColumnName, newValue);
                        }

                        if (ObjectUtils.notEqual(oldValue, newValue))
                        {
                            dataChanged = true;
                            break;
                        }
                    }

                    if (!dataChanged)
                    {
                        // No need to update the data, because there's no changes
                        continue;
                    }
                }

                performUpdate(nameValues);
            }
            else
            {
                // Note: If INSERT batch will be flushed during this run,
                // we may need to re-select remaining records from nameValuesBuffer.
                // To avoid that we temporary disable batch executions
                performInsert(nameValues);
            }
        }
    }

    private Map<String, Map<String, Object>> toPrimaryKeysHashMap(ResultSet resultSet) throws SQLException
    {
        final Map<String, Map<String, Object>> result = new HashMap<>();

        while (resultSet.next())
        {
            Map<String, Object> parsedResultSet = new HashMap<String, Object>();

            for (String columnName : getOrderedTableColumnNames())
            {
                parsedResultSet.put(columnName, resultSet.getObject(columnName));
            }

            result.put(config.joinPrimaryKeys(parsedResultSet), parsedResultSet);
        }

        return result;
    }

    private ResultSet selectBatch() throws SQLException, ConfigurationException, ScriptException
    {
        PreparedStatement selectStatement = getOrCreateSelectStatement(nameValuesBuffer.size());

        selectStatement.clearParameters();

        int parameterIndex = 1;

        for (int i = 0; i < nameValuesBuffer.size(); i++)
        {
            Map<String, Object> nameValues = nameValuesBuffer.get(i).getValue();
            for (String primaryKeyColumnName : config.getPrimaryKeys())
            {
                Object columnValue = transform(primaryKeyColumnName, nameValues);

                if (Import.isVerboseEnabled())
                {
                    printNameValue(primaryKeyColumnName, columnValue);
                }

                selectStatement.setObject(parameterIndex++, columnValue);
            }
        }

        return Import.measureTime(selectStatementTimer, new Callable<ResultSet>()
        {
            @Override
            public ResultSet call() throws SQLException
            {
                return selectStatement.executeQuery();
            }
        });
    }

    private boolean addBatch(Map<String, Object> nameValues) throws InterruptedException
    {
        String keys = config.joinPrimaryKeys(nameValues);

        // Re-route early before strategy closed and while not all consumers were shutdown

        // XXX Copy-paste:
        // XXX Check duplicates should be performed on eval'ed/transformed values,
        // right now it's partially true (i.e. eval'ed but not transformed values are used),
        // and only if map function is declared in configuration
        if (config.isIgnoreDuplicatePK())
        {
            // If needed re-route this to another handler based on keys hash

            int partitionId = Math.abs(keys.hashCode() % threadCount);

            if (partitionId != threadId)
            {
                router.dispatch(nameValues, partitionId);
                return true;
            }
        }

        nameValuesBuffer.add(Pair.of(keys, nameValues));

        return nameValuesBuffer.size() < config.getBatchSize();
    }

    protected abstract void performInsert(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException, InterruptedException;

    protected abstract void performUpdate(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException;

    @Override
    public void close()
    {
        try
        {
            if (!nameValuesBuffer.isEmpty())
            {
                throw new IllegalStateException("Subclasses should flush batched records prior to close");
            }
        }
        finally
        {
            for (Entry<Integer, PreparedStatement> entry : selectStatements.entrySet())
            {
                closeQuietly(entry.getValue());
            }

            super.close();
        }
    }
}