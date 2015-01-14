package com.anjlab.csv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

public class Importer
{

    private final Configuration config;
    private final FileResolver scriptingResolver;
    private final int numberOfThreads;

    public Importer(Configuration config, int numberOfThreads, FileResolver scriptingResolver)
    {
        this.numberOfThreads = numberOfThreads;
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
        // each thread will take batch of lines with the size of batchSize from the queue,
        // that's why it's necessary always to have enough lines for those who read from it's thread
        int queueSize = config.getBatchSize() * numberOfThreads;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        final ScriptEngine engine = loadScriptEngine();
        final BlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(queueSize);
        final String terminalMessage = UUID.randomUUID().toString();

        for (int i = 0; i < numberOfThreads; i++)
        {
            final RecordHandler strategy = getRecordHandlerStrategy(createConnection(), engine);

            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String[] nextLine = queue.take();
                        while (nextLine.length != 0 && !terminalMessage.equals((nextLine)[0]))
                        {
                            // nextLine[] is an array of values from the line
                            Map<String, String> nameValues = new HashMap<String, String>();
                            for (Map.Entry<Integer, String> mapping : config.getColumnMappings().entrySet())
                            {
                                String value = nextLine[mapping.getKey()];

                                String targetColumnName = mapping.getValue();

                                nameValues.put(targetColumnName, value);
                            }
                            strategy.handleRecord(nameValues);
                            nextLine = queue.take();
                        }
                        queue.put(new String[] { terminalMessage });
                    }
                    catch (BatchUpdateException bue)
                    {
                        bue.printStackTrace(System.err);
                        SQLException se = bue.getNextException();
                        while (se != null)
                        {
                            System.err.println("Next SQLException in chain:");
                            se.printStackTrace(System.err);
                            se = se.getNextException();
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace(System.err);
                    }
                    finally
                    {
                        try
                        {
                            strategy.close();
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException("Problem has occurred while closing resources.", e);
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        CSVReader reader = null;
        try
        {
            Configuration.CSVOptions csvOptions = config.getCsvOptions();
            reader = new CSVReader(new InputStreamReader(input),
                    csvOptions.getSeparatorChar(),
                    csvOptions.getQuoteChar(),
                    csvOptions.getEscapeChar(),
                    csvOptions.getSkipLines(),
                    csvOptions.isStrictQuotes(),
                    csvOptions.isIgnoreLeadingWhiteSpace());

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null)
            {
                // XXX This may block if all handlers terminated with error
                queue.put(nextLine);
            }
            queue.put(new String[] { terminalMessage });
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            System.err.println("Interrupted");
        }
        finally
        {
            if(reader != null)
            {
                reader.close();
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

    public Connection createConnection() throws ClassNotFoundException, SQLException
    {
        Class.forName(config.getDriverClass());
        Properties properties = new Properties();
        if (config.getConnectionProperties() != null)
        {
            properties.putAll(config.getConnectionProperties());
        }

        return DriverManager.getConnection(config.getConnectionUrl(), properties);
    }

    private RecordHandler getRecordHandlerStrategy(Connection connection, ScriptEngine scriptEngine) throws SQLException, ScriptException
    {
        switch (config.getOperationMode())
        {
            case INSERT:
                return new InsertRecordHandler(config, connection, scriptEngine);
            default:
                return new MergeRecordHandler(config, connection, scriptEngine);
        }
    }

}

