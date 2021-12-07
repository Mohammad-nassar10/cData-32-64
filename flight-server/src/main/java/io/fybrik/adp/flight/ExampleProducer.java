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
    private final int RecordsPerBatch = 1024*1024;
    private final VectorSchemaRoot constVectorSchemaRoot;
    private boolean isNonBlocking = false;

    public ExampleProducer(Location location, BufferAllocator allocator, Transformer transformer) {
        this.location = location;
        this.allocator = allocator;
        this.transformer = transformer;
        this.constVectorSchemaRoot = this.getConstVectorSchemaRoot();
                // System.out.println("vsr producer = " + this.constVectorSchemaRoot.contentToTSVString());

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
            c.setSafe(j, (long)j+1);
            d.setSafe(j, (long)j);
            ++j;
        }
    }

    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        Runnable loadData = () -> {
            listener.setUseZeroCopy(true);
            VectorSchemaRoot transformedRoot = null;
            for(int i = 0; i < 5000; ++i) {
                System.out.println(i);
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
// 143282 released size = 9410
// 140840 released size = 7528
// 138398 released size = 5646
// 135956 released size = 3764
// 133514 released size = 1882

// 141922 released size = 8050
// 139752 released size = 6440
// 137582 released size = 4830
// 135412 released size = 3220
// 133242 released size = 1610

// 141922 released size = 10850
// 139752 released size = 8680
// 137582 released size = 6510
// 135412 released size = 4340
// 133242 released size = 2170

// 135923882 released size = 68814458
// 137761108 released size = 70651124
// 139598334 released size = 72487790

// 1071954 released size = 544866
// 1041064 released size = 514536
// 1010174 released size = 484206

// 154942 released size = 18942
// 152772 released size = 17220

// 154942 released size = 23870
// 152772 released size = 21700

// 10981072 released size = 10750000
// 10978902 released size = 10747850

// 2216888256 released size = 2146731776
// 2216230806 released size = 2146075288
// 2215573356 released size = 2145418800

// 2217545706 released size = 2150373462
// 2216888256 released size = 2149716032
// 2216230806 released size = 2149058602

// 2217545706 released size = 2150436842
// 2216888256 released size = 2149779392
// 2216230806 released size = 2149121942

// 1433005438 released size = 1365896574
// 1431168212 released size = 1364059348