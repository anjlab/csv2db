package com.anjlab.csv2db;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractRecordHandler implements RecordHandler
{
    protected Configuration config;

    protected final ScriptEngine scriptEngine;

    private Set<String> transientColumns;

    protected Connection connection;

    public AbstractRecordHandler(Configuration config, ScriptEngine scriptEngine, Connection connection)
    {
        this.config = config;
        this.scriptEngine = scriptEngine;
        this.connection = connection;
        this.transientColumns = new HashSet<String>();
        if (config.getTransientColumns() != null)
        {
            this.transientColumns.addAll(config.getTransientColumns());
        }
    }

    protected void closeQuietly(PreparedStatement statement)
    {
        try
        {
            statement.close();
        }
        catch (SQLException e)
        {
            //  Ignore
        }
    }

    protected void closeQuietly(ResultSet resultSet)
    {
        try
        {
            resultSet.close();
        }
        catch (SQLException e)
        {
            //  Ignore
        }
    }

    protected void closeQuietly(Connection connection)
    {
        try
        {
            connection.close();
        }
        catch (SQLException e)
        {
            //  Ignore
        }
    }

    private List<String> orderedTableColumnNames;

    protected List<String> getOrderedTableColumnNames()
    {
        if (orderedTableColumnNames != null)
        {
            return orderedTableColumnNames;
        }

        List<Integer> csvColumnIndices = new ArrayList<Integer>();
        csvColumnIndices.addAll(config.getColumnMappings().keySet());
        Collections.sort(csvColumnIndices);

        List<String> columnNames = new ArrayList<String>();

        for (int csvColumnIndex : csvColumnIndices)
        {
            String columnName = config.getColumnMappings().get(csvColumnIndex);

            if (!isTransientColumn(columnName))
            {
                columnNames.add(columnName);
            }
        }

        return orderedTableColumnNames = columnNames;
    }

    protected boolean isTransientColumn(String columnName)
    {
        return transientColumns.contains(columnName);
    }

    private List<String> columnNamesWithInsertValues;

    protected List<String> getColumnNamesWithInsertValues()
    {
        if (columnNamesWithInsertValues != null)
        {
            return columnNamesWithInsertValues;
        }

        if (config.getInsertValues() == null)
        {
            return columnNamesWithInsertValues = Collections.emptyList();
        }

        List<String> columnNames = new ArrayList<String>();
        columnNames.addAll(config.getInsertValues().keySet());
        Collections.sort(columnNames);

        return columnNamesWithInsertValues = columnNames;
    }

    private List<String> columnNamesWithUpdateValues;

    protected List<String> getColumnNamesWithUpdateValues()
    {
        if (columnNamesWithUpdateValues != null)
        {
            return columnNamesWithUpdateValues;
        }

        if (config.getUpdateValues() == null)
        {
            return columnNamesWithUpdateValues = Collections.emptyList();
        }

        List<String> columnNames = new ArrayList<String>();
        columnNames.addAll(config.getUpdateValues().keySet());
        Collections.sort(columnNames);

        return columnNamesWithUpdateValues = columnNames;
    }

    protected Object transform(String targetTableColumnName, Map<String, Object> nameValues) throws ConfigurationException, ScriptException
    {
        if (config.getTransform() != null)
        {
            ValueDefinition transformer =
                    config.getTransform().get(targetTableColumnName);

            if (transformer != null)
            {
                if (transformer.producesSQL())
                {
                    throw new ConfigurationException(
                            "Transform definition for column '" + targetTableColumnName + "' produces SQL which is not supported. "
                                    + "SQL expressions only supported for 'insertValues' and 'updateValues'.");
                }

                return transformer.eval(targetTableColumnName, nameValues, scriptEngine);
            }
        }

        return nameValues.get(targetTableColumnName);
    }

    @Override
    public void close()
    {
        closeQuietly(connection);
    }
}