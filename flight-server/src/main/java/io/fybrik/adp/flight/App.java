package io.fybrik.adp.flight;
import java.io.IOException;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.AllocationManager.Factory;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.allocator.WasmAllocationFactory;
import io.fybrik.adp.core.transformer.IPCTransformer;
import io.fybrik.adp.core.transformer.NoOpTransformer;
import io.fybrik.adp.core.transformer.Transformer;
import io.fybrik.adp.core.transformer.WasmTransformer;
public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("hello world");
        Options options = new Options();
        options.addOption("h", "host", true, "Host");
        options.addOption("p", "port", true, "Port");
        options.addOption("t", "transform", true, "Transformation WASM module (optional)");
        options.addOption("i", "ipc", true, "using IPC (optional)");
        System.out.println("gg");
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);
        String host = line.getOptionValue("host", "localhost");
        int port = Integer.parseInt(line.getOptionValue("port", "49152"));
        String transform = line.getOptionValue("transform", "");
        Boolean ipc = Boolean.valueOf(line.getOptionValue("ipc", "false"));
        Instance instance = null;
        RootAllocator allocator;
        Transformer transformer;

        // ipc = true;
        if (transform != null && !transform.isEmpty() && ipc == false) {
            System.out.printf("Creating instance for %s%n", transform);
            instance = new Instance(transform);
            System.out.println("Creating WasmAllocationFactory");
            Factory allocationFactory = new WasmAllocationFactory(instance);
            System.out.println("Creating RootAllocator");
            allocator = new RootAllocator(RootAllocator.configBuilder().allocationManagerFactory(allocationFactory).build());
            System.out.println("Creating WasmTransformer");
            transformer = new WasmTransformer(allocator, instance);
        } else if (transform != null && !transform.isEmpty() && ipc == true) {
            System.out.printf("Creating instance for %s for IPC%n", transform);
            instance = new Instance(transform);
            System.out.println("Creating WasmAllocationFactory");
            Factory allocationFactory = new WasmAllocationFactory(instance);
            System.out.println("Creating RootAllocator");
            allocator = new RootAllocator(RootAllocator.configBuilder().allocationManagerFactory(allocationFactory).build());
            System.out.println("Creating IPCTransformer");
            transformer = new IPCTransformer(allocator, instance);
        } else {
            allocator = new RootAllocator(Long.MAX_VALUE);
            transformer = new NoOpTransformer();
        }

        System.out.printf("Listening %s:%d%n", host, port);
        Location location = Location.forGrpcInsecure(host, port);
        try (ExampleProducer producer = new ExampleProducer(location, allocator, transformer)) {
            // VectorSchemaRoot input = producer.getConstVectorSchemaRoot();
            // System.out.println("original = " + input.contentToTSVString());
            int i = 0;
            VectorSchemaRoot transformedRoot = null;
            long start = System.currentTimeMillis();
            for(i = 0; i < 10000; ++i) {
                System.out.println(i);
                producer.transformer.next();
                transformedRoot = producer.transformer.root();
                // System.out.println("transformed = " + transformedRoot.contentToTSVString());
                producer.transformer.releaseHelpers();
                // producer.transformer.close();
            }
            long finish = System.currentTimeMillis();
            System.out.println("Time spent traversing dataset: " + (finish - start) / 1000.0 + " seconds");
            double throughput = (double) i * transformedRoot.getRowCount() * 4 * 4 / ((finish - start) / 1000.0);
            System.out.println("Throughput: " + String.format("%.2f", throughput / (1024 * 1024)) + "MB/sec");
            // producer.close();
        }
    }
}
