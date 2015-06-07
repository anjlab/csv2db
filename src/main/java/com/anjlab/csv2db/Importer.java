package com.anjlab.csv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
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
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

public class Importer
{

    private final Configuration config;
    private final int numberOfThreads;

    public Importer(Configuration config, int numberOfThreads)
    {
        this.numberOfThreads = numberOfThreads;
        this.config = config;
    }

    public void performImport(String filename)
            throws ClassNotFoundException, SQLException, IOException, ScriptException, ConfigurationException
    {
        performImport(filename, new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return true;
            }
        });
    }

    public void performImport(String filename, FilenameFilter filenameFilter)
            throws ClassNotFoundException, SQLException, IOException, ScriptException,
            ConfigurationException
    {
        final File inputFile = new File(filename);

        if (inputFile.isDirectory())
        {
            importFromDir(inputFile, filenameFilter);
        }
        else if (StringUtils.endsWithIgnoreCase(filename, ".zip"))
        {
            importFromZip(inputFile, filenameFilter);
        }
        else
        {
            System.out.println("Importing from " + inputFile.getName() + "...");
            performImport(new AutoCloseInputStream(new FileInputStream(inputFile)));
        }
    }

    private void importFromDir(final File input, FilenameFilter filenameFilter)
            throws ClassNotFoundException, SQLException, IOException,
            ScriptException, ConfigurationException, FileNotFoundException
    {
        for (File file : input.listFiles())
        {
            if (file.isFile() && filenameFilter.accept(null, file.getName()))
            {
                System.out.println("Importing from " + file.getName() + "...");
                performImport(new AutoCloseInputStream(new FileInputStream(file)));
            }
        }
    }

    private void importFromZip(final File inputFile, FilenameFilter filenameFilter)
            throws ZipException, IOException, ClassNotFoundException,
            SQLException, ScriptException, ConfigurationException
    {
        try (ZipFile zipFile = new ZipFile(inputFile))
        {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();

            while (entries.hasMoreElements())
            {
                ZipEntry entry = entries.nextElement();

                if (!entry.isDirectory() && filenameFilter.accept(null, entry.getName()))
                {
                    System.out.println("Importing from " + entry.getName() + "...");
                    performImport(zipFile.getInputStream(entry));
                }
            }
        }
    }

    public void performImport(InputStream input) throws ClassNotFoundException, SQLException, IOException, ScriptException, ConfigurationException
    {
        // each thread will take batch of lines with the size of batchSize from the queue,
        // that's why it's necessary always to have enough lines for those who read from it's thread
        int queueSize = config.getBatchSize() * numberOfThreads;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        final BlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(queueSize);
        final String terminalMessage = UUID.randomUUID().toString();

        for (int i = 0; i < numberOfThreads; i++)
        {
            final RecordHandler strategy = getRecordHandlerStrategy(createConnection(), config.getScriptEngine());

            // The map function accepts nameValues and the JavaScript emit callback function.
            // The emit function should call back to Java, but since we can't create pure Java
            // object representing JavaScript function we create this bridge that will in turn
            // do the actual call to Java using the #handleRecord(...) interface method
            final Object emitFunction;
            {
                String threadLocalEmit = "emit" + i;
                String threadLocalStrategy = "strategy" + i;

                StringBuilder emitFunctionDeclaration = new StringBuilder()
                        .append("function ").append(threadLocalEmit).append("(nameValues) {")
                        .append(threadLocalStrategy).append(".handleRecord(nameValues);")
                        .append("}");

                try
                {
                    config.getScriptEngine().getContext().setAttribute(
                            threadLocalStrategy, strategy, ScriptContext.ENGINE_SCOPE);

                    emitFunction = config.getScriptEngine().eval(emitFunctionDeclaration.toString());
                }
                catch (ScriptException e)
                {
                    throw new RuntimeException("Internal error", e);
                }
            }

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
                            Map<String, Object> nameValues = new HashMap<String, Object>();
                            for (Map.Entry<Integer, String> mapping : config.getColumnMappings().entrySet())
                            {
                                String value = nextLine[mapping.getKey()];

                                String targetColumnName = mapping.getValue();

                                nameValues.put(targetColumnName, value);
                            }

                            if (config.getMap() == null)
                            {
                                strategy.handleRecord(nameValues);
                            }
                            else
                            {
                                // Note that all emitted values (if any)
                                // will be handled by this same thread
                                config.getMap().eval(
                                        config.getScriptEngine(),
                                        nameValues,
                                        emitFunction);
                            }

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
                    catch (Throwable e)
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

    public Connection createConnection() throws ClassNotFoundException, SQLException, ConfigurationException
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
        case INSERTONLY:
            return new InsertOnlyRecordHandler(config, connection, scriptEngine);
        default:
            return new MergeRecordHandler(config, connection, scriptEngine);
        }
    }

}

