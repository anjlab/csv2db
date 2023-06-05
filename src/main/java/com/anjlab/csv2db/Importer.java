package com.anjlab.csv2db;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.StringUtils;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.Closeable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Importer
{

    private final Configuration config;
    private final int numberOfThreads;
    private final PerformanceCounter perfCounter;

    public Importer(Configuration config, int numberOfThreads, PerformanceCounter perfCounter)
    {
        this.numberOfThreads = numberOfThreads;
        this.config = config;
        this.perfCounter = perfCounter;
    }

    public void performImport(String filename)
            throws ClassNotFoundException, SQLException, IOException,
            ScriptException, ConfigurationException, InterruptedException,
            ArchiveException
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
            ConfigurationException, InterruptedException, ArchiveException
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
            logImportingFrom(inputFile.getName());

            performImport(new AutoCloseInputStream(new FileInputStream(inputFile)));
        }
    }

    private void importFromDir(final File input, FilenameFilter filenameFilter)
            throws ClassNotFoundException, SQLException, IOException,
            ScriptException, ConfigurationException, FileNotFoundException, InterruptedException
    {
        for (File file : input.listFiles())
        {
            if (file.isFile() && filenameFilter.accept(null, file.getName()))
            {
                logImportingFrom(file.getName());

                performImport(new AutoCloseInputStream(new FileInputStream(file)));
            }
        }
    }

    private void importFromZip(final File inputFile, FilenameFilter filenameFilter)
            throws IOException, ClassNotFoundException,
            SQLException, ScriptException, ConfigurationException, InterruptedException, ArchiveException
    {
        try (ArchiveInputStream archiveInput = new ArchiveStreamFactory()
                .createArchiveInputStream(
                        FilenameUtils.getExtension(inputFile.getName()),
                        new AutoCloseInputStream(new FileInputStream(inputFile))))
        {
            while (true)
            {
                ArchiveEntry entry = archiveInput.getNextEntry();
                if (entry != null)
                {
                    if (!entry.isDirectory() && filenameFilter.accept(null, entry.getName()))
                    {
                        logImportingFrom(entry.getName());

                        performImport(archiveInput);
                    }
                }
                else
                {
                    break;
                }
            }
        }
    }

    private void logImportingFrom(String name)
    {
        System.out.println("\nImporting from '" + name + "'...");
    }

    public void performImport(InputStream input)
            throws ClassNotFoundException, SQLException, IOException, ScriptException, ConfigurationException, InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        Mediator mediator = new SharedBlockingQueueMediator(config, numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++)
        {
            executorService.submit(createConsumer(mediator, i));
        }

        executorService.shutdown();

        readInput(input, mediator);

        executorService.awaitTermination(1, TimeUnit.DAYS);
    }

    private void readInput(InputStream input, Mediator mediator) throws InterruptedException
    {
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

            long counter = 0;

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null)
            {
                // XXX This may block if all handlers terminated with error
                mediator.dispatch(nextLine);

                if (perfCounter != null)
                {
                    perfCounter.lineEnqueued();
                }

                if (config.getLimit() > 0)
                {
                    counter++;

                    if (counter >= config.getLimit())
                    {
                        Import.logVerbose("Finishing importer early after " + counter + " lines");
                        break;
                    }
                }
            }
            mediator.producerDone();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(reader);
        }
    }

    private void closeQuietly(Closeable closeable)
    {
        if (closeable != null)
        {
            try
            {
                closeable.close();
            }
            catch (IOException e)
            {
                e.printStackTrace(System.err);
            }
        }
    }

    private Runnable createConsumer(final Mediator mediator, final int threadId)
            throws SQLException, ScriptException, ClassNotFoundException, ConfigurationException
    {
        return new Runnable()
        {
            final RecordHandler strategy = getRecordHandlerStrategy(
                    createConnection(), config.getScriptEngine(), mediator, threadId);

            final Timer recordsMeter;

            // TODO No need building & binding emitFunction if `config.getMap() == null`

            // The map function accepts nameValues and the JavaScript emit callback function.
            // The emit function should call back to Java, but since we can't create pure Java
            // object representing JavaScript function we create this bridge that will in turn
            // do the actual call to Java using the #handleRecord(...) interface method
            final Object emitFunction;

            {
                String threadLocalEmit = "emit" + threadId;
                String threadLocalStrategy = "strategy" + threadId;

                StringBuilder emitFunctionDeclaration = new StringBuilder()
                        .append("function ").append(threadLocalEmit).append("(nameValues) {")
                        .append(threadLocalStrategy).append(".handleRecord(nameValues);")
                        .append("};")
                        .append(threadLocalEmit);

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

                recordsMeter = Import.isMetricsEnabled()
                        ? Import.METRIC_REGISTRY.timer("thread-" + threadId + ".records")
                        : null;
            }

            @Override
            public void run()
            {
                try
                {
                    readLines(mediator, threadId);
                }
                catch (Throwable t)
                {
                    printStackTrace(t);
                }
                finally
                {
                    try
                    {
                        strategy.close();
                    }
                    catch (Exception e)
                    {
                        printStackTrace(e);
                    }
                }
            }

            private void readLines(final Mediator mediator, final int threadId)
                    throws InterruptedException, SQLException, ConfigurationException, ScriptException
            {
                Object next = mediator.take(threadId);

                while (true)
                {
                    Context time = null;

                    if (recordsMeter != null)
                    {
                        time = recordsMeter.time();
                    }

                    try
                    {
                        if (!handleRecord(next))
                        {
                            break;
                        }

                        next = mediator.take(threadId);
                    }
                    finally
                    {
                        if (time != null)
                        {
                            time.stop();
                        }
                    }
                }
            }

            @SuppressWarnings("unchecked")
            private boolean handleRecord(Object record)
                    throws SQLException, ConfigurationException, ScriptException, InterruptedException
            {
                if (record instanceof String[])
                {
                    // record is an array of values from CSV line
                    String[] columns = (String[]) record;

                    if (columns.length == 0)
                    {
                        return false;
                    }

                    Map<String, Object> nameValues = new HashMap<String, Object>();
                    for (Map.Entry<Integer, String> mapping : config.getColumnMappings().entrySet())
                    {
                        String value = columns[mapping.getKey()];

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
                }
                else if (record instanceof Map)
                {
                    // re-routed record
                    strategy.handleRecord((Map<String, Object>) record);
                }
                else
                {
                    // null-value?
                    return false;
                }

                return true;
            }

            private void printStackTrace(Throwable t)
            {
                if (t instanceof BatchUpdateException)
                {
                    printBatchUpdateException((BatchUpdateException) t);
                }
                else if (t.getCause() instanceof BatchUpdateException)
                {
                    printBatchUpdateException((BatchUpdateException) t.getCause());
                }
                else
                {
                    t.printStackTrace(System.err);
                }
            }

            private void printBatchUpdateException(BatchUpdateException bue)
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
        };
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

    private RecordHandler getRecordHandlerStrategy(
            Connection connection, ScriptEngine scriptEngine,
            Router router, int threadId)
                    throws SQLException, ScriptException
    {
        switch (config.getOperationMode())
        {
        case INSERT:
            return new InsertRecordHandler(config, connection, scriptEngine, router, threadId, numberOfThreads);
        case INSERTONLY:
            return new InsertOnlyRecordHandler(config, connection, scriptEngine, router, threadId, numberOfThreads);
        default:
            return new MergeRecordHandler(config, connection, scriptEngine, router, threadId, numberOfThreads);
        }
    }

}

