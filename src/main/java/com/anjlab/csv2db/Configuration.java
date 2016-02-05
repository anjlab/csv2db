package com.anjlab.csv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.input.AutoCloseInputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

public class Configuration
{
    private static final String MODE = "mode";
    private static final String DRIVER_CLASS = "driverClass";
    private static final String CONNECTION_URL = "connectionUrl";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private static final int DEFAULT_BATCH_SIZE = 100;

    private static final Gson gson = createGson();

    private static Gson createGson()
    {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(ValueDefinition.class, new ValueDefinitionAdapter());
        return gsonBuilder.create();
    }

    public enum OperationMode
    {
        INSERT, MERGE, INSERTONLY
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
    private Map<String, ValueDefinition> connectionProperties;
    private String targetTable;
    private List<String> primaryKeys;
    /**
     * Map keys are zero-based column indices in CSV file.
     * Map values are target table column names.
     */
    private Map<Integer, String> columnMappings;
    private List<String> transientColumns;
    private List<String> syntheticColumns;
    private Map<String, ValueDefinition> insertValues;
    private Map<String, ValueDefinition> updateValues;
    private Map<String, ValueDefinition> transform;
    private ValueDefinition map;
    private List<String> scripting;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private CSVOptions csvOptions;
    private boolean forceUpdate;
    private boolean ignoreNullPK;

    private transient FileResolver fileResolver;
    private transient ScriptEngine scriptEngine;

    public static Configuration fromJson(String filename) throws FileNotFoundException
    {
        FileResolver resolverRelativeToParentFolder =
                new SimpleFileResolver(new File(filename).getParentFile());

        return fromJson(new AutoCloseInputStream(new FileInputStream(new File(filename))),
                resolverRelativeToParentFolder);
    }

    public static Configuration fromJson(InputStream input, FileResolver fileResolver) throws FileNotFoundException
    {
        Configuration config = gson.fromJson(
                readConfig(input, fileResolver), Configuration.class);

        if (config.getCsvOptions() == null)
        {
            config.setCsvOptions(new CSVOptions());
        }

        config.setFileResolver(fileResolver);

        return config;
    }

    private static JsonElement readConfig(InputStream input, FileResolver fileResolver) throws FileNotFoundException
    {
        JsonObject config = (JsonObject) new JsonParser().parse(new InputStreamReader(input));

        if (config.has("extend"))
        {
            String parentFilename = config.get("extend").getAsString();

            JsonElement parentConfig = readConfig(
                    new AutoCloseInputStream(new FileInputStream(fileResolver.getFile(parentFilename))),
                    fileResolver);

            return extend(config, parentConfig);
        }

        return config;
    }

    private static JsonElement extend(JsonElement configElement, JsonElement parentConfigElement)
    {
        if (!configElement.isJsonObject() || !parentConfigElement.isJsonObject())
        {
            return configElement;
        }

        final JsonObject config = configElement.getAsJsonObject();
        final JsonObject parentConfig = parentConfigElement.getAsJsonObject();

        for (Entry<String, JsonElement> entry : parentConfig.entrySet())
        {
            if (!config.has(entry.getKey()))
            {
                // Copy entire member from parent
                config.add(entry.getKey(), entry.getValue());
            }
            else
            {
                // Same property declared, but maybe some children don't have overrides?
                extend(config.get(entry.getKey()), entry.getValue());
            }
        }

        return config;
    }

    public String toJson()
    {
        return gson.toJson(this);
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

    public List<String> getSyntheticColumns()
    {
        return syntheticColumns;
    }

    public void setSyntheticColumns(List<String> syntheticColumns)
    {
        this.syntheticColumns = syntheticColumns;
    }

    public FunctionReference getMap()
    {
        return (FunctionReference) map;
    }

    public void setMap(FunctionReference map)
    {
        this.map = map;
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

    public Map<String, String> getConnectionProperties() throws ConfigurationException
    {
        Map<String, String> properties = new HashMap<String, String>();
        for (Entry<String, ValueDefinition> entry : connectionProperties.entrySet())
        {
            try
            {
                ValueDefinition value = entry.getValue();

                if (value.producesSQL())
                {
                    throw new ConfigurationException(
                            "Connection property '" + entry.getKey() + "' produces SQL which is not supported."
                                    + "Only primitive types and function references allowed here.");
                }

                properties.put(
                        entry.getKey(),
                        String.valueOf(
                                value.eval(
                                        entry.getKey(),
                                        new HashMap<String, Object>(),
                                        getScriptEngine())));
            }
            catch (ScriptException e)
            {
                throw new RuntimeException("Error evaluating connection properties", e);
            }
        }
        return properties;
    }

    public void setConnectionProperties(Map<String, ValueDefinition> connectionProperties)
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

    public boolean isIgnoreNullPK()
    {
        return ignoreNullPK;
    }

    public void setIgnoreNullPK(boolean ignoreNullPK)
    {
        this.ignoreNullPK = ignoreNullPK;
    }

    public FileResolver getFileResolver()
    {
        return fileResolver;
    }

    public void setFileResolver(FileResolver fileResolver)
    {
        this.fileResolver = fileResolver;
    }

    public ScriptEngine getScriptEngine()
    {
        if (scriptEngine == null)
        {
            try
            {
                scriptEngine = newScriptEngine();
            }
            catch (ScriptException | IOException e)
            {
                throw new RuntimeException("Error loading scripting engine", e);
            }
        }
        return scriptEngine;
    }

    private ScriptEngine newScriptEngine() throws FileNotFoundException,
            ScriptException, IOException
    {
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("Nashorn");

        if (scriptEngine == null)
        {
            scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
        }

        if (getScripting() != null)
        {
            for (String filename : getScripting())
            {
                FileReader scriptReader = new FileReader(fileResolver.getFile(filename));
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

    public static void addOptions(Options options)
    {
        options
                .addOption("m", MODE, true, "Operation mode (INSERT, MERGE, INSERTONLY)")
                .addOption("d", DRIVER_CLASS, true, "JDBC driver class name")
                .addOption("l", CONNECTION_URL, true, "JDBC connection URL")
                .addOption("u", USERNAME, true, "Connection username")
                .addOption("p", PASSWORD, true, "Connection password");
    }

    public Configuration overrideFrom(CommandLine cmd)
    {
        if (cmd.hasOption(MODE))
        {
            setOperationMode(OperationMode.valueOf(cmd.getOptionValue(MODE)));
        }

        if (cmd.hasOption(DRIVER_CLASS))
        {
            setDriverClass(cmd.getOptionValue(DRIVER_CLASS));
        }

        if (cmd.hasOption(CONNECTION_URL))
        {
            setConnectionUrl(cmd.getOptionValue(CONNECTION_URL));
        }

        if (cmd.hasOption(USERNAME))
        {
            connectionProperties.put("username", new StringLiteral(cmd.getOptionValue(USERNAME)));
        }

        if (cmd.hasOption(PASSWORD))
        {
            connectionProperties.put("password", new StringLiteral(cmd.getOptionValue(PASSWORD)));
        }

        if (cmd.hasOption(Import.BATCH_SIZE))
        {
            setBatchSize(Integer.parseInt(cmd.getOptionValue(Import.BATCH_SIZE, "1")));
        }

        return this;
    }
}
