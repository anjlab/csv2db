package com.anjlab.csv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.AutoCloseInputStream;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

import com.google.gson.Gson;

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
    private Map<String, String> defaultValues;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private CSVOptions csvOptions;
    
    public static Configuration fromJson(String filename) throws FileNotFoundException
    {
        return fromJson(new AutoCloseInputStream(new FileInputStream(new File(filename))));
    }
    
    public static Configuration fromJson(InputStream input)
    {
        Configuration config = new Gson().fromJson(new InputStreamReader(input), Configuration.class);
        
        if (config.getCsvOptions() == null)
        {
            config.setCsvOptions(new CSVOptions());
        }
        
        return config;
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
    
    public Map<String, String> getDefaultValues()
    {
        return defaultValues;
    }
    
    public void setDefaultValues(Map<String, String> defaultValues)
    {
        this.defaultValues = defaultValues;
    }
    
    public int getBatchSize()
    {
        return batchSize <= 0 ? 1 : batchSize;
    }
    
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }
}
