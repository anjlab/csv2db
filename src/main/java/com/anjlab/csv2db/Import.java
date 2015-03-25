package com.anjlab.csv2db;

import java.io.IOException;
import java.sql.SQLException;

import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class Import
{

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, SQLException, ScriptException, ConfigurationException
    {
        Options options =
                new Options()
                        .addOption("i", "input", true, "Input CSV file")
                        .addOption("c", "config", true, "Configuration file")
                        .addOption("t", "numberOfThreads", true, "Number of threads (default is number of processors available to JVM)")
                        .addOption("h", "help", false, "Prints this help");

        Configuration.addOptions(options);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        final int numberOfThreads;
        try
        {
            cmd = parser.parse(options, args);

            if (!cmd.hasOption("config") || !cmd.hasOption("input"))
            {
                printHelp(options);
                System.exit(1);
                return;
            }

            final int availableProcessors = Runtime.getRuntime().availableProcessors();

            numberOfThreads = cmd.hasOption("numberOfThreads")
                    ? // number of threads will be in range [1; availableProcessors]
                    Math.max(1, Math.min(Integer.parseInt(cmd.getOptionValue("numberOfThreads")), availableProcessors))
                    : availableProcessors;
        }
        catch (Exception e)
        {
            e.printStackTrace(System.err);
            printHelp(options);
            System.exit(1);
            return;
        }
        if (cmd.hasOption("help"))
        {
            printHelp(options);
            System.exit(0);
            return;
        }

        String configFilename = cmd.getOptionValue("config");

        Configuration config = Configuration.fromJson(configFilename).overrideFrom(cmd);

        Importer importer = new Importer(config, numberOfThreads);

        importer.performImport(cmd.getOptionValue("input"));
    }

    private static void printHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("./run.sh", options);
    }

}
