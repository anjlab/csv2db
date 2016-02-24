package com.anjlab.csv2db;

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

import javax.script.ScriptEngine;
import javax.script.ScriptException;

public abstract class AbstractRecordHandler implements RecordHandler
{
    private Set<String> transientColumns;

    private List<String> orderedTableColumnNames;

    private List<String> columnNamesWithInsertValues;

    private List<String> columnNamesWithUpdateValues;

    protected Configuration config;

    protected final ScriptEngine scriptEngine;

    protected Connection connection;

    protected boolean batchExecutionDisabled;

    protected final Router router;

    protected final int threadId;

    protected final int threadCount;

    public AbstractRecordHandler(
            Configuration config,
            ScriptEngine scriptEngine,
            Connection connection,
            Router router,
            int threadId,
            int threadCount)
    {
        this.config = config;
        this.scriptEngine = scriptEngine;
        this.connection = connection;
        this.transientColumns = new HashSet<String>();
        if (config.getTransientColumns() != null)
        {
            this.transientColumns.addAll(config.getTransientColumns());
        }
        this.router = router;
        this.threadId = threadId;
        this.threadCount = threadCount;
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

        if (config.getSyntheticColumns() != null)
        {
            List<String> copy = new ArrayList<>();
            copy.addAll(config.getSyntheticColumns());
            Collections.sort(copy);

            for (String columnName : copy)
            {
                // There shouldn't be any transient columns between synthetic ones,
                // but we'll check this anyway for consistency
                if (!isTransientColumn(columnName))
                {
                    columnNames.add(columnName);
                }
            }
        }

        return orderedTableColumnNames = columnNames;
    }

    protected boolean isTransientColumn(String columnName)
    {
        return transientColumns.contains(columnName);
    }

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

                try
                {
                    return transformer.eval(targetTableColumnName, nameValues, scriptEngine);
                }
                catch (RuntimeException | ScriptException e)
                {
                    System.err.println("Error running transformation for column '" + targetTableColumnName + "'");
                    throw e;
                }
            }
        }

        return nameValues.get(targetTableColumnName);
    }

    protected Object eval(ValueDefinition definition, String targetTableColumnName, Map<String, Object> nameValues)
            throws ScriptException
    {
        try
        {
            return definition.eval(targetTableColumnName, nameValues, scriptEngine);
        }
        catch (RuntimeException | ScriptException e)
        {
            System.err.println("Error evaluating value for column '" + targetTableColumnName + "'");
            throw e;
        }
    }

    @Override
    public void close()
    {
        closeQuietly(connection);
    }

    protected static void printNameValues(Map<String, Object> nameValues)
    {
        List<String> names = new ArrayList<>();
        names.addAll(nameValues.keySet());
        Collections.sort(names);

        for (String name : names)
        {
            printNameValue(name, nameValues.get(name));
        }
    }

    protected static void printNameValue(String name, Object value)
    {
        Import.logVerbose(name + "=" +
                (value == null
                        ? "null"
                        : "'" + value + "', class=" + value.getClass().getName()));
    }

    protected void enableBatchExecution() throws SQLException
    {
        batchExecutionDisabled = false;
    }

    protected void disableBatchExecution()
    {
        batchExecutionDisabled = true;
    }

}