package com.anjlab.csv2db;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Pattern;

import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class Import
{

    private static final String CONFIG = "config";
    private static final String HELP = "help";
    private static final String INCLUDE = "include";
    private static final String INPUT = "input";
    private static final String NUMBER_OF_THREADS = "numberOfThreads";
    private static final String SKIP = "skip";

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, SQLException, ScriptException, ConfigurationException
    {
        Options options =
                new Options()
                        .addOption("i", INPUT, true,
                                "Input CSV file, or ZIP containing CSV files, or path to folder that contains CSV files")
                        .addOption("n", INCLUDE, true,
                                "Only process files whose names match this regexp (matches all files in input ZIP or input folder by default)")
                        .addOption("s", SKIP, true,
                                "Skip files whose names match this regexp (skip nothing by default)")
                        .addOption("c", CONFIG, true, "Configuration file")
                        .addOption("t", NUMBER_OF_THREADS, true, "Number of threads"
                                + " (default is number of processors available to JVM)")
                        .addOption("h", HELP, false, "Prints this help");

        Configuration.addOptions(options);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        final int numberOfThreads;
        final Pattern include;
        final Pattern skip;

        try
        {
            cmd = parser.parse(options, args);

            if (cmd.hasOption(HELP))
            {
                printHelp(options);
                System.exit(0);
                return;
            }

            if (!cmd.hasOption(CONFIG) || !cmd.hasOption(INPUT))
            {
                printHelp(options);
                System.exit(1);
                return;
            }

            final int availableProcessors = Runtime.getRuntime().availableProcessors();

            numberOfThreads = cmd.hasOption(NUMBER_OF_THREADS)
                    ? // number of threads will be in range [1; availableProcessors]
                    Math.max(1, Math.min(Integer.parseInt(cmd.getOptionValue(NUMBER_OF_THREADS)), availableProcessors))
                    : availableProcessors;

            include = cmd.hasOption(INCLUDE)
                    ? Pattern.compile(cmd.getOptionValue(INCLUDE))
                    : null;

            skip = cmd.hasOption(SKIP)
                    ? Pattern.compile(cmd.getOptionValue(SKIP))
                    : null;
        }
        catch (Exception e)
        {
            e.printStackTrace(System.err);
            printHelp(options);
            System.exit(1);
            return;
        }

        String configFilename = cmd.getOptionValue(CONFIG);

        Configuration config = Configuration.fromJson(configFilename).overrideFrom(cmd);

        Importer importer = new Importer(config, numberOfThreads);

        importer.performImport(cmd.getOptionValue(INPUT), new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                if (skip != null && skip.matcher(name).find())
                {
                    System.out.println("Skipping " + name);
                    return false;
                }

                boolean accept = include == null || include.matcher(name).find();
                if (!accept)
                {
                    System.out.println("Skipping " + name);
                }
                return accept;
            }
        });
    }

    private static void printHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("./run.sh", options);
    }

}
