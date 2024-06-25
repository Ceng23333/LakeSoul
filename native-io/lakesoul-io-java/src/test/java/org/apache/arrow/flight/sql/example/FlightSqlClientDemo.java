package org.apache.arrow.flight.sql.example;

import org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.cli.*;

import java.util.ArrayList;


public class FlightSqlClientDemo {
    public static void main(final String[] args) throws Exception {
        final Options options = new Options();

        options.addRequiredOption("host", "host", true, "Host to connect to");
        options.addRequiredOption("port", "port", true, "Port to connect to");
        options.addRequiredOption("command", "command", true, "Method to run");

        options.addOption("query", "query", true, "Query");
        options.addOption("catalog", "catalog", true, "Catalog");
        options.addOption("schema", "schema", true, "Schema");
        options.addOption("table", "table", true, "Table");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        ArrayList<String> argsList = new ArrayList<>();
        argsList.add("--host");
        argsList.add("localhost");

        argsList.add("--port");
        argsList.add("50051");

        argsList.add("--command");
//        argsList.add("GetCatalogs");
//        argsList.add("GetPrimaryKeys");
        argsList.add("Execute");

        argsList.add("--query");
        argsList.add("select * from table");

        argsList.add("--catalog");
        argsList.add("catalog");

        argsList.add("--schema");
        argsList.add("schema");

        argsList.add("--table");
        argsList.add("table");

        try {
            cmd = parser.parse(options, argsList.toArray(new String[0]));
            try (final FlightSqlClientDemoApp thisApp =
                         new FlightSqlClientDemoApp(new RootAllocator(Integer.MAX_VALUE))) {
                thisApp.executeApp(cmd);
            }

        } catch (final ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("FlightSqlClientDemoApp -host localhost -port 32010 ...", options);
            throw e;
        }
    }

}
