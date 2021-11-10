package io.fybrik.adp.flight;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Token;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.fybrik.adp.core.transformer.Transformer;

/**
 *  Flight Producer for flight server that returns a single dataset.
 *  The dataset consists of a 1000 identical record batches. The record
 *  batch is in memory, has 4 integer columns of size 1024*1024.
 */
public class ExampleProducer extends NoOpFlightProducer implements AutoCloseable {
    private final Location location;
    private final BufferAllocator allocator;
    private final Transformer transformer;
    private final int RecordsPerBatch = 100;
    private final VectorSchemaRoot constVectorSchemaRoot;
    private boolean isNonBlocking = false;

    public ExampleProducer(Location location, BufferAllocator allocator, Transformer transformer) {
        this.location = location;
        this.allocator = allocator;
        this.transformer = transformer;
        this.constVectorSchemaRoot = this.getConstVectorSchemaRoot();

        this.transformer.init(this.constVectorSchemaRoot);
    }

    private VectorSchemaRoot getConstVectorSchemaRoot() {
        BigIntVector a = new BigIntVector("a", this.allocator);
        BigIntVector b = new BigIntVector("b", this.allocator);
        BigIntVector c = new BigIntVector("c", this.allocator);
        BigIntVector d = new BigIntVector("d", this.allocator);
        a.allocateNew();
        b.allocateNew();
        c.allocateNew();
        d.allocateNew();
        int j = 0;

        while(true) {
            if (j >= RecordsPerBatch) {
                a.setValueCount(RecordsPerBatch);
                b.setValueCount(RecordsPerBatch);
                c.setValueCount(RecordsPerBatch);
                d.setValueCount(RecordsPerBatch);
                List<Field> fields = Arrays.asList(a.getField(), b.getField(), c.getField(), d.getField());
                List<FieldVector> vectors = Arrays.asList(a, b, c, d);
                VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors);
                // System.out.println("vsr producer = " + vsr.contentToTSVString());
                return vsr;
            }

            a.setSafe(j, (long)j);
            b.setSafe(j, (long)j);
            c.setSafe(j, (long)j);
            d.setSafe(j, (long)j);
            ++j;
        }
    }

    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        Runnable loadData = () -> {
            listener.setUseZeroCopy(true);
            VectorSchemaRoot transformedRoot = null;
            for(int i = 0; i < 100; ++i) {
                // System.out.println("init = " + this.transformer.originalRoot().contentToTSVString());
                this.transformer.next();
                // System.out.println("get stream = " + this.transformer.root().contentToTSVString());
                if (transformedRoot == null) {
                    transformedRoot = this.transformer.root();
                    if (transformedRoot != null) {
                        listener.start(transformedRoot);
                    }
                }

                if (transformedRoot != null) {
                    listener.putNext();
                }
            }

            listener.completed();
        };
        if (!this.isNonBlocking) {
            loadData.run();
        } else {
            ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit(loadData);
            service.shutdown();
        }

    }

    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        Schema pojoSchema = this.constVectorSchemaRoot.getSchema();
        Token token = Token.newBuilder().build();
        Ticket ticket = new Ticket(token.toByteArray());
        List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, new Location[]{this.location}));
        return new FlightInfo(pojoSchema, descriptor, endpoints, -1L, -1L);
    }

    public void close() throws Exception {
        if (this.transformer != null) {
            this.transformer.close();
        }

        this.constVectorSchemaRoot.close();
    }
}
