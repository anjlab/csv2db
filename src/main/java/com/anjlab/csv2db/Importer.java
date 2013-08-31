package com.anjlab.csv2db;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

import com.anjlab.csv2db.Configuration.CSVOptions;

public class Importer
{

    public abstract class AbstractRecordHandler implements RecordHandler
    {
        protected final ScriptEngine scriptEngine;
        
        private Set<String> transientColumns;
        
        public AbstractRecordHandler(ScriptEngine scriptEngine)
        {
            this.scriptEngine = scriptEngine;
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

        protected Object transform(String targetTableColumnName, Map<String, String> nameValues) throws ConfigurationException, ScriptException
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
    }
    
    public class MergeRecordHandler extends AbstractRecordHandler
    {
        private AbstractRecordHandler insertRecordHandler;
        
        private PreparedStatement selectStatement;
        private PreparedStatement updateStatement;
        
        public MergeRecordHandler(Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
        {
            super(scriptEngine);
            
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
            
            this.insertRecordHandler = new InsertRecordHandler(connection, scriptEngine);
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

    public class InsertRecordHandler extends AbstractRecordHandler
    {
        private PreparedStatement insertStatement;
        
        public InsertRecordHandler(Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
        {
            super(scriptEngine);
            
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
            
            insertStatement = connection.prepareStatement(insertClause.toString());
        }

        private int numberOfStatementsInBatch;
        
        @Override
        public void handleRecord(Map<String, String> nameValues) throws SQLException, ConfigurationException, ScriptException
        {
            int parameterIndex = 1;
            
            for (String targetTableColumnName : getColumnNamesWithInsertValues())
            {
                ValueDefinition definition = config.getInsertValues().get(targetTableColumnName);
                
                if (!definition.producesSQL())
                {
                    Object columnValue = definition.eval(targetTableColumnName, nameValues, scriptEngine);
                    
                    insertStatement.setObject(parameterIndex++, columnValue);
                }
            }
            
            for (String targetTableColumnName : getOrderedTableColumnNames())
            {
                insertStatement.setObject(parameterIndex++, transform(targetTableColumnName, nameValues));
            }
            
            numberOfStatementsInBatch++;
            
            insertStatement.addBatch();
            
            checkBatchExecution(config.getBatchSize());
        }

        private void checkBatchExecution(int limit) throws SQLException
        {
            if (numberOfStatementsInBatch > limit)
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
            }
        }
    }

    public interface RecordHandler extends Closeable
    {
        void handleRecord(Map<String, String> nameValues) throws SQLException, ConfigurationException, ScriptException;
    }

    private final Configuration config;
    private final FileResolver scriptingResolver;
    
    public Importer(Configuration config, FileResolver scriptingResolver)
    {
        this.config = config;
        this.scriptingResolver = scriptingResolver;
    }
    
    public void performImport(String filename) throws ClassNotFoundException, SQLException, IOException, ScriptException, ConfigurationException
    {
        //  Support reading ZIP archives
        if (StringUtils.endsWithIgnoreCase(filename, ".zip"))
        {
            ZipFile zipFile = null;
            try
            {
                zipFile = new ZipFile(new File(filename));
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements())
                {
                    ZipEntry entry = entries.nextElement();
                    
                    performImport(zipFile.getInputStream(entry));
                }
            }
            finally
            {
                if (zipFile != null)
                {
                    zipFile.close();
                }
            }
        }
        else
        {
            InputStream inputStream = new FileInputStream(new File(filename));
            performImport(new AutoCloseInputStream(inputStream));
        }
    }
    
    public void performImport(InputStream input) throws ClassNotFoundException, SQLException, IOException, ScriptException, ConfigurationException
    {
        Connection connection = getConnection();
        
        RecordHandler strategy = null;
        
        CSVReader reader = null;
        CSVOptions csvOptions = config.getCsvOptions();
        try
        {
            reader = new CSVReader(new InputStreamReader(input),
                    csvOptions.getSeparatorChar(),
                    csvOptions.getQuoteChar(),
                    csvOptions.getEscapeChar(),
                    csvOptions.getSkipLines(),
                    csvOptions.isStrictQuotes(),
                    csvOptions.isIgnoreLeadingWhiteSpace());
            
            strategy = getRecordHandlerStrategy(connection, loadScriptEngine());
            
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null)
            {
                // nextLine[] is an array of values from the line
                
                Map<String, String> nameValues = new HashMap<String, String>();
                
                for (Entry<Integer, String> mapping : config.getColumnMappings().entrySet())
                {
                    String value = nextLine[mapping.getKey()];
                    
                    String targetColumnName = mapping.getValue();
                    
                    nameValues.put(targetColumnName, value);
                }
                
                strategy.handleRecord(nameValues);
            }
        }
        finally
        {
            if (strategy != null)
            {
                strategy.close();
            }
            if (reader != null)
            {
                IOUtils.closeQuietly(reader);
            }
            if (autocloseConnection)
            {
                connection.close();
            }
        }
    }

    private ScriptEngine loadScriptEngine() throws FileNotFoundException,
            ScriptException, IOException
    {
        ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("JavaScript");
        
        if (config.getScripting() != null)
        {
            for (String filename : config.getScripting())
            {
                FileReader scriptReader = new FileReader(scriptingResolver.getFile(filename));
                try
                {
                    scriptEngine.eval(scriptReader);
                }
                finally
                {
                    scriptReader.close();
                }
            }
        }
        
        return scriptEngine;
    }
    
    private boolean autocloseConnection = true;
    
    public boolean isAutocloseConnection()
    {
        return autocloseConnection;
    }
    
    public void setAutocloseConnection(boolean autocloseConnection)
    {
        this.autocloseConnection = autocloseConnection;
    }
    
    private Connection connection;
    
    public Connection getConnection() throws ClassNotFoundException, SQLException
    {
        if (connection == null)
        {
            Class.forName(config.getDriverClass());
            
            Properties properties = new Properties();
            if (config.getConnectionProperties() != null)
            {
                properties.putAll(config.getConnectionProperties());
            }
            
            connection = DriverManager.getConnection(config.getConnectionUrl(), properties);
        }
        return connection;
    }

    private RecordHandler getRecordHandlerStrategy(Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
    {
        switch (config.getOperationMode())
        {
        case INSERT:
            return new InsertRecordHandler(connection, scriptEngine);
        default:
            return new MergeRecordHandler(connection, scriptEngine);
        }
    }

}
