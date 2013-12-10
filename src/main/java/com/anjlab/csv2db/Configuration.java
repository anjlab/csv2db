package com.anjlab.csv2db;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.input.AutoCloseInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class Configuration
{

    private static final int DEFAULT_BATCH_SIZE = 100;

    public enum OperationMode
    {
        INSERT, MERGE
    }

    public static class CSVOptions
    {
        private char separatorChar = CSVParser.DEFAULT_SEPARATOR;
        private char quoteChar = CSVParser.DEFAULT_QUOTE_CHARACTER;
        private char escapeChar = CSVParser.DEFAULT_ESCAPE_CHARACTER;
        private int skipLines = CSVReader.DEFAULT_SKIP_LINES;
        private boolean strictQuotes = CSVParser.DEFAULT_STRICT_QUOTES;
        private boolean ignoreLeadingWhiteSpace = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;

        public char getSeparatorChar()
        {
            return separatorChar;
        }

        public void setSeparatorChar(char separatorChar)
        {
            this.separatorChar = separatorChar;
        }

        public char getQuoteChar()
        {
            return quoteChar;
        }

        public void setQuoteChar(char quoteChar)
        {
            this.quoteChar = quoteChar;
        }

        public char getEscapeChar()
        {
            return escapeChar;
        }

        public void setEscapeChar(char escapeChar)
        {
            this.escapeChar = escapeChar;
        }

        public int getSkipLines()
        {
            return skipLines;
        }

        public void setSkipLines(int skipLines)
        {
            this.skipLines = skipLines;
        }

        public boolean isStrictQuotes()
        {
            return strictQuotes;
        }

        public void setStrictQuotes(boolean strictQuotes)
        {
            this.strictQuotes = strictQuotes;
        }

        public boolean isIgnoreLeadingWhiteSpace()
        {
            return ignoreLeadingWhiteSpace;
        }

        public void setIgnoreLeadingWhiteSpace(boolean ignoreLeadingWhiteSpace)
        {
            this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        }
    }

    private OperationMode operationMode;
    private String driverClass;
    private String connectionUrl;
    private Map<String, String> connectionProperties;
    private String targetTable;
    private List<String> primaryKeys;
    /**
     * Map keys are zero-based column indices in CSV file.
     * Map values are target table column names.
     */
    private Map<Integer, String> columnMappings;
    private List<String> transientColumns;
    private Map<String, ValueDefinition> insertValues;
    private Map<String, ValueDefinition> updateValues;
    private Map<String, ValueDefinition> transform;
    private List<String> scripting;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private CSVOptions csvOptions;
    private boolean forceUpdate;

    public static Configuration fromJson(String filename) throws FileNotFoundException
    {
        return fromJson(new AutoCloseInputStream(new FileInputStream(new File(filename))));
    }

    public static Configuration fromJson(InputStream input)
    {
        Configuration config = createGson().fromJson(new InputStreamReader(input), Configuration.class);

        if (config.getCsvOptions() == null)
        {
            config.setCsvOptions(new CSVOptions());
        }

        return config;
    }

    public String toJson()
    {
        return createGson().toJson(this);
    }

    private static Gson createGson()
    {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(ValueDefinition.class, new ValueDefinitionAdapter());
        return gsonBuilder.create();
    }

    public String getDriverClass()
    {
        return driverClass;
    }

    public void setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
    }

    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
    }

    public Map<Integer, String> getColumnMappings()
    {
        return columnMappings;
    }

    public void setColumnMappings(Map<Integer, String> columnMappings)
    {
        this.columnMappings = columnMappings;
    }

    public List<String> getTransientColumns()
    {
        return transientColumns;
    }

    public void setTransientColumns(List<String> transientColumns)
    {
        this.transientColumns = transientColumns;
    }

    public CSVOptions getCsvOptions()
    {
        return csvOptions;
    }

    public void setCsvOptions(CSVOptions csvOptions)
    {
        this.csvOptions = csvOptions;
    }

    public List<String> getPrimaryKeys()
    {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys)
    {
        this.primaryKeys = primaryKeys;
    }

    public Map<String, String> getConnectionProperties()
    {
        return connectionProperties;
    }

    public void setConnectionProperties(Map<String, String> connectionProperties)
    {
        this.connectionProperties = connectionProperties;
    }

    public String getTargetTable()
    {
        return targetTable;
    }

    public void setTargetTable(String targetTable)
    {
        this.targetTable = targetTable;
    }

    public OperationMode getOperationMode()
    {
        return operationMode;
    }

    public void setOperationMode(OperationMode operationMode)
    {
        this.operationMode = operationMode;
    }

    public Map<String, ValueDefinition> getInsertValues()
    {
        return insertValues;
    }

    public void setInsertValues(Map<String, ValueDefinition> insertValues)
    {
        this.insertValues = insertValues;
    }

    public Map<String, ValueDefinition> getUpdateValues()
    {
        return updateValues;
    }

    public void setUpdateValues(Map<String, ValueDefinition> updateValues)
    {
        this.updateValues = updateValues;
    }

    public Map<String, ValueDefinition> getTransform()
    {
        return transform;
    }

    public void setTransform(Map<String, ValueDefinition> transform)
    {
        this.transform = transform;
    }

    public List<String> getScripting()
    {
        return scripting;
    }

    public void setScripting(List<String> scripting)
    {
        this.scripting = scripting;
    }

    public int getBatchSize()
    {
        return batchSize <= 0 ? 1 : batchSize;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public boolean isForceUpdate()
    {
        return forceUpdate;
    }

    public void setForceUpdate(boolean forceUpdate)
    {
        this.forceUpdate = forceUpdate;
    }
}
