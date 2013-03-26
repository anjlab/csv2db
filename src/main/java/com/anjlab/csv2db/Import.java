package com.anjlab.csv2db;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Import
{
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, SQLException
    {
        Options options =
                new Options()
                    .addOption("i", "input", true, "Input CSV file")
                    .addOption("c", "config", true, "Configuration file")
                    .addOption("h", "help", false, "Prints this help");
        
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try
        {
            cmd = parser.parse(options, args);
            
            if (!cmd.hasOption("config") || !cmd.hasOption("input"))
            {
                printHelp(options);
                System.exit(1);
                return;
            }
        }
        catch (ParseException e)
        {
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
        
        Configuration config = Configuration.fromJson(cmd.getOptionValue("config"));
        
        new Importer(config).performImport(cmd.getOptionValue("input"));
    }

    private static void printHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("./run.sh", options);
    }
}
