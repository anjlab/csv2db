package com.anjlab.csv2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractInsertUpdateRecordHandler extends AbstractRecordHandler
{
    private Map<Integer, PreparedStatement> selectStatements;

    private StringBuilder whereClause;

    private final List<Pair<Integer, Map<String, Object>>> nameValuesBuffer;

    public AbstractInsertUpdateRecordHandler(Configuration config, ScriptEngine scriptEngine, Connection connection) throws SQLException
    {
        super(config, scriptEngine, connection);

        if (config.getPrimaryKeys() == null || config.getPrimaryKeys().isEmpty())
        {
            throw new RuntimeException("primaryKeys required for " + config.getOperationMode() + " mode");
        }

        nameValuesBuffer = new ArrayList<>(config.getBatchSize());

        selectStatements = new HashMap<>();
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
                            .append(StringUtils.join(getOrderedTableColumnNames(), ", "))
                            .append(" FROM ")
                            .append(config.getTargetTable())
                            .append(" WHERE ")
                            .append(buildWhereClause());

            for (int i = 1; i < batchSize; i++)
            {
                selectClause.append(" OR (")
                        .append(buildWhereClause())
                        .append(")");
            }

            statement = connection.prepareStatement(selectClause.toString());

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

                whereClause.append(columnName).append(" = ?");
            }
        }

        return whereClause;
    }

    @Override
    public void handleRecord(Map<String, Object> nameValues) throws SQLException, ConfigurationException, ScriptException
    {
        if (addBatch(nameValues))
        {
            return;
        }

        executeBatch();
    }

    protected void executeBatch() throws SQLException, ConfigurationException, ScriptException
    {
        if (nameValuesBuffer.isEmpty())
        {
            return;
        }

        try (ResultSet resultSet = selectBatch())
        {
            handleRecordsBatch(toPrimaryKeysHashMap(resultSet));
        }
        finally
        {
            nameValuesBuffer.clear();
        }
    }

    private void handleRecordsBatch(Map<Integer, Map<String, Object>> primaryKeysHashMap)
            throws SQLException, ConfigurationException, ScriptException
    {
        for (Pair<Integer, Map<String, Object>> pair : nameValuesBuffer)
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

                        if (ObjectUtils.notEqual(oldValue, newValue))
                        {
                            dataChanged = true;
                            break;
                        }
                    }

                    if (!dataChanged)
                    {
                        // No need to update the data, because there's no changes
                        return;
                    }
                }

                performUpdate(nameValues);
            }
            else
            {
                performInsert(nameValues);
            }
        }
    }

    private Map<Integer, Map<String, Object>> toPrimaryKeysHashMap(ResultSet resultSet) throws SQLException
    {
        final Map<Integer, Map<String, Object>> result = new HashMap<>();

        while (resultSet.next())
        {
            Map<String, Object> parsedResultSet = new HashMap<String, Object>();

            for (String columnName : getOrderedTableColumnNames())
            {
                parsedResultSet.put(columnName, resultSet.getObject(columnName));
            }

            result.put(hashPrimaryKeys(parsedResultSet), parsedResultSet);
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
                selectStatement.setObject(parameterIndex++, transform(primaryKeyColumnName, nameValues));
            }
        }

        return selectStatement.executeQuery();
    }

    private boolean addBatch(Map<String, Object> nameValues)
    {
        nameValuesBuffer.add(Pair.of(hashPrimaryKeys(nameValues), nameValues));

        return nameValuesBuffer.size() < config.getBatchSize();
    }

    private int hashPrimaryKeys(Map<String, Object> nameValues)
    {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(11, 17);
        for (String primaryKeyColumnName : config.getPrimaryKeys())
        {
            hashCodeBuilder.append(ObjectUtils.defaultIfNull(nameValues.get(primaryKeyColumnName), "null"));
        }
        return hashCodeBuilder.toHashCode();
    }

    protected abstract void performInsert(Map<String, Object> nameValues)
            throws SQLException, ConfigurationException, ScriptException;

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