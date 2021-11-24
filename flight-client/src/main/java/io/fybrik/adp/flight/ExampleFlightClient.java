package io.fybrik.adp.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.nio.charset.StandardCharsets;

public class ExampleFlightClient {

    /**
     * Accepts the hostname and port of the flight server. Reads the record-batches
     * one at a time. measures time and computes throughput.
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Host");
        options.addOption("p", "port", true, "Port");
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);
        String host = line.getOptionValue("host", "localhost");
        int port = Integer.valueOf(line.getOptionValue("port", "49152"));

        final BufferAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = rootAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
        final FlightClient client = FlightClient.builder().allocator(allocator)
                .location(Location.forGrpcInsecure(host, port)).build();

        final CallHeaders callHeaders = new FlightCallHeaders();
        final HeaderCallOption clientProperties = new HeaderCallOption(callHeaders);
        FlightDescriptor flightDescriptor = FlightDescriptor.command("123".getBytes(StandardCharsets.UTF_8));
        FlightInfo flightInfo = client.getInfo(flightDescriptor, clientProperties);
        System.out.println(flightInfo.getSchema());
        FlightEndpoint flightEndpoint = flightInfo.getEndpoints().get(0);
        Ticket ticket = flightEndpoint.getTicket();
        System.out.println(ticket);

        FlightStream s = client.getStream(ticket);
        int i = 0;
        VectorSchemaRoot root = null;
        
        long start = System.currentTimeMillis();
        while (s.next()) {
            // System.out.println("get stream");
            root = s.getRoot();
            i++;
            System.out.println(i);
            // System.out.println(root.getVector(0));
            // System.out.println(root.getVector(1));
            // System.out.println(root.getVector(2));
            // System.out.println(root.getVector(3));
        }
        long finish = System.currentTimeMillis();

        System.out.println("done");
        if (root != null) {
            System.out.println("Time spent traversing dataset: " + (finish - start) / 1000.0 + " seconds");
            double throughput = (double) i * root.getRowCount() * 4 * 4 / ((finish - start) / 1000.0);
            System.out.println("Throughput: " + String.format("%.2f", throughput / (1024 * 1024)) + "MB/sec");    
        }
        s.close();
        client.close();

        System.out.println("closing");
        rootAllocator.close();
        System.out.println("closed");
    }
}
