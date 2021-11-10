package io.fybrik.adp.core.transformer;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.jni.JniWrapper;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
// import org.apache.arrow..ArrowArray;
// import org.apache.arrow.ffi.ArrowSchema;
// import org.apache.arrow.ffi.FFI;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

public class WasmTransformer implements Transformer {
    private final BufferAllocator allocator;
    private final long instancePtr;
    private VectorSchemaRoot originalRoot;
    private VectorSchemaRoot transformedRoot;
    private boolean closed;

    public WasmTransformer(BufferAllocator allocator, Instance instance) {
        this.allocator = allocator;
        this.instancePtr = instance.getInstancePtr();
    }

    public void init(VectorSchemaRoot root) {
        Preconditions.checkState(this.originalRoot == null, "init can only be called once");
        this.originalRoot = root;
    }

    public VectorSchemaRoot originalRoot() {
        return this.originalRoot;
    }

    public VectorSchemaRoot root() {
        return this.transformedRoot;
    }

    public void next() {
        long base = 0L;
        long arrayPtr = 0L;
        long schemaPtr = 0L;
        // Create a contex with empty schema and array
        long context = JniWrapper.get().prepare(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        // // System.exit(0);
        
        // long context = JniWrapper.get().prepare(instancePtr);
        // long base = JniWrapper.get().wasmMemPtr(this.instancePtr);

        System.out.println("fill input schema");
        // long schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        ArrowSchema inputSchema = ArrowSchema.wrap(base + schemaPtr);
        System.out.println("next input schema " + inputSchema.snapshot().format + ", origin schema " + originalRoot.getSchema());
        // Data.exportSchema(allocator, originalRoot.getSchema(), null, inputSchema);
        System.out.println("fill input array, original root = ");
        // long arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        ArrowArray inputArray = ArrowArray.wrap(base + arrayPtr);
        // Data.exportVectorSchemaRoot(allocator, originalRoot, null, inputArray);
        // Use Java c data to fill the schema and the array with the original root
        Data.exportVectorSchemaRoot(allocator, originalRoot, null, inputArray, inputSchema);
        System.out.println("next input schema private data " + inputSchema.snapshot().release + ", origin schema " + originalRoot.getSchema());
        System.out.println("arrow array " + inputArray.snapshot().length);
        
        // VectorSchemaRoot roundTrip = Data.importVectorSchemaRoot(allocator, inputArray, inputSchema, null);
        // System.out.println("round trip " + roundTrip.contentToTSVString());
        //// convert in arcg.rs
        // JniWrapper.get().convert_to_32(instancePtr, context);
        

        JniWrapper.get().transform(instancePtr, context);
        
        // // TODO: read output


        System.out.println("finish");
        JniWrapper.get().finish(instancePtr, context);
        System.out.println("next completed");
    }

    public void close() throws Exception {
        if (!this.closed) {
            if (this.transformedRoot != null) {
                this.transformedRoot.close();
            }

            this.closed = true;
        }
    }
}
