package com.anjlab.csv2db;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.AutoCloseInputStream;

import au.com.bytecode.opencsv.CSVReader;

import com.anjlab.csv2db.Configuration.CSVOptions;

public class Importer
{

    public abstract class AbstractRecordHandler implements RecordHandler
    {
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
        
        protected List<String> getOrderedTableColumnNames()
        {
            List<Integer> csvColumnIndices = new ArrayList<Integer>();
            csvColumnIndices.addAll(config.getColumnMappings().keySet());
            Collections.sort(csvColumnIndices);
            
            List<String> columnNames = new ArrayList<String>();
            
            for (int csvColumnIndex : csvColumnIndices)
            {
                columnNames.add(config.getColumnMappings().get(csvColumnIndex));
            }
            return columnNames;
        }
    }
    
    public class MergeRecordHandler extends AbstractRecordHandler implements Closeable
    {
        private RecordHandler insertRecordHandler;
        
        private PreparedStatement selectStatement;
        private PreparedStatement updateStatement;
        
        public MergeRecordHandler(Connection connection) throws SQLException
        {
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
            
            this.insertRecordHandler = new InsertRecordHandler(connection);
        }
        
        @Override
        public void close()
        {
            closeQuietly(selectStatement);
        }

        @Override
        public void handleRecord(Map<String, String> nameValues) throws SQLException
        {
            selectStatement.clearParameters();
            updateStatement.clearParameters();
            
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
                    //  Perform update
                    parameterIndex = 1;
                    
                    //  Set parameters for the SET clause
                    for (String targetTableColumnName : getOrderedTableColumnNames())
                    {
                        updateStatement.setObject(parameterIndex++, nameValues.get(targetTableColumnName));
                    }
                    
                    //  Set parameters for the WHERE clause
                    for (String primaryKeyColumnName : config.getPrimaryKeys())
                    {
                        Object primaryKeyColumnValue = resultSet.getObject(primaryKeyColumnName);
                        
                        updateStatement.setObject(parameterIndex++, primaryKeyColumnValue);
                    }
                    
                    long rowsUpdated = updateStatement.executeUpdate();
                    
                    assert rowsUpdated == 1;
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

    }

    public class InsertRecordHandler extends AbstractRecordHandler implements Closeable
    {
        private PreparedStatement insertStatement;
        
        public InsertRecordHandler(Connection connection) throws SQLException
        {
            StringBuilder insertClause =
                    new StringBuilder("INSERT INTO ")
                              .append(config.getTargetTable())
                              .append(" (");
            
            StringBuilder valuesClause = new StringBuilder();
            
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

        @Override
        public void handleRecord(Map<String, String> nameValues) throws SQLException
        {
            insertStatement.clearParameters();
            
            int parameterIndex = 1;
            
            for (String targetTableColumnName : getOrderedTableColumnNames())
            {
                String columnValue = nameValues.get(targetTableColumnName);
                
                insertStatement.setObject(parameterIndex++, columnValue);
            }
            
            insertStatement.execute();
        }
        
        @Override
        public void close()
        {
            closeQuietly(insertStatement);
        }
    }

    public interface RecordHandler
    {
        void handleRecord(Map<String, String> nameValues) throws SQLException;
    }

    private Configuration config;
    
    public Importer(Configuration config)
    {
        this.config = config;
    }

    public void performImport(String filename) throws ClassNotFoundException, SQLException, IOException
    {
        performImport(new AutoCloseInputStream(new FileInputStream(new File(filename))));
    }
    
    public void performImport(InputStream input) throws ClassNotFoundException, SQLException, IOException
    {
        Connection connection = getConnection();
        
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
            
            RecordHandler strategy = getRecordHandlerStrategy(connection);
            
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

    private RecordHandler getRecordHandlerStrategy(Connection connection) throws SQLException
    {
        switch (config.getOperationMode())
        {
        case INSERT:
            return new InsertRecordHandler(connection);
        default:
            return new MergeRecordHandler(connection);
        }
    }

}
