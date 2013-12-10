package com.anjlab.csv2db;


import org.apache.commons.lang3.ObjectUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class MergeRecordHandler extends AbstractRecordHandler
{
    private AbstractRecordHandler insertRecordHandler;

    private PreparedStatement selectStatement;
    private PreparedStatement updateStatement;

    public MergeRecordHandler(Configuration config, Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
    {
        super(config, scriptEngine, connection);

        if (config.getPrimaryKeys() == null || config.getPrimaryKeys().isEmpty())
        {
            throw new RuntimeException("primaryKeys required for MERGE mode");
        }

        StringBuilder whereClause = new StringBuilder();

        for (String columnName : config.getPrimaryKeys())
        {
            if (whereClause.length() > 0)
            {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName).append(" = ?");
        }

        StringBuilder selectClause =
                new StringBuilder("SELECT * FROM ")
                        .append(config.getTargetTable())
                        .append(" WHERE ")
                        .append(whereClause);

        this.selectStatement = connection.prepareStatement(selectClause.toString());

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
                        .append(whereClause);

        this.updateStatement = connection.prepareStatement(updateClause.toString());

        this.insertRecordHandler = new InsertRecordHandler(config, connection, scriptEngine);
    }

    @Override
    public void close()
    {
        try
        {
            checkBatchExecution(0);

            insertRecordHandler.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(selectStatement);
            closeQuietly(updateStatement);
            super.close();
        }
    }

    private int numberOfStatementsInBatch;

    @Override
    public void handleRecord(Map<String, String> nameValues) throws SQLException, ConfigurationException, ScriptException
    {
        selectStatement.clearParameters();

        int parameterIndex = 1;

        for (String primaryKeyColumnName : config.getPrimaryKeys())
        {
            String primaryKeyColumnValue = nameValues.get(primaryKeyColumnName);

            selectStatement.setObject(parameterIndex++, primaryKeyColumnValue);
        }

        ResultSet resultSet = selectStatement.executeQuery();

        try
        {
            if (resultSet.next())
            {
                if (!config.isForceUpdate())
                {
                    boolean dataChanged = false;

                    for (String targetTableColumnName : getOrderedTableColumnNames())
                    {
                        Object oldValue = resultSet.getObject(targetTableColumnName);
                        Object newValue = transform(targetTableColumnName, nameValues);

                        if (!ObjectUtils.equals(oldValue, newValue))
                        {
                            dataChanged = true;
                            break;
                        }
                    }

                    if (!dataChanged)
                    {
                        //  No need to update the data, because there's no changes
                        return;
                    }
                }

                //  Perform update
                parameterIndex = 1;

                //  Set parameters for the SET clause
                for (String targetTableColumnName : getColumnNamesWithUpdateValues())
                {
                    ValueDefinition definition = config.getUpdateValues().get(targetTableColumnName);

                    if (!definition.producesSQL())
                    {
                        Object columnValue = definition.eval(targetTableColumnName, nameValues, scriptEngine);

                        updateStatement.setObject(parameterIndex++, columnValue);
                    }
                }

                for (String targetTableColumnName : getOrderedTableColumnNames())
                {
                    updateStatement.setObject(parameterIndex++, transform(targetTableColumnName, nameValues));
                }

                //  Set parameters for the WHERE clause
                for (String primaryKeyColumnName : config.getPrimaryKeys())
                {
                    Object primaryKeyColumnValue = resultSet.getObject(primaryKeyColumnName);

                    updateStatement.setObject(parameterIndex++, primaryKeyColumnValue);
                }

                numberOfStatementsInBatch++;

                updateStatement.addBatch();

                checkBatchExecution(config.getBatchSize());
            }
            else
            {
                //  Perform insert
                insertRecordHandler.handleRecord(nameValues);
            }
        }
        finally
        {
            closeQuietly(resultSet);
        }
    }

    private void checkBatchExecution(int limit) throws SQLException
    {
        if (numberOfStatementsInBatch > limit)
        {
            updateStatement.executeBatch();

            updateStatement.clearParameters();

            numberOfStatementsInBatch = 0;
        }
    }

}