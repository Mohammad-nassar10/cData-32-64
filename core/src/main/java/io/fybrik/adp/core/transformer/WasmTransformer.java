package io.fybrik.adp.core.transformer;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.jni.JniWrapper;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

public class WasmTransformer implements Transformer {
    private final BufferAllocator allocator;
    private final long instancePtr;
    private VectorSchemaRoot originalRoot;
    private VectorSchemaRoot transformedRoot;
    private boolean closed;

    //////////
    private long context;
    private long schemaPtr;
    private long arrayPtr;
    //////////

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
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        // System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        
        ArrowSchema inputSchema = ArrowSchema.wrap(base + schemaPtr);
        ArrowArray inputArray = ArrowArray.wrap(base + arrayPtr);
        // Use Java c data to fill the schema and the array with the original root
        Data.exportVectorSchemaRoot(allocator, originalRoot, null, inputArray, inputSchema);

        // System.out.println("before tansform");
        // Call the transform function from Rust side
        JniWrapper.get().convert64To32(instancePtr, context);
        // long transformResultPtr = JniWrapper.get().transform(instancePtr, context);
        JniWrapper.get().transform(instancePtr, context);
        long transformResultPtr = JniWrapper.get().convert32To64(instancePtr, context);

        // read transformed vector
        long out_schema = JniWrapper.get().getOutputSchema(transformResultPtr);
        long out_array = JniWrapper.get().getOutputArray(transformResultPtr);
        ArrowSchema outSchema = ArrowSchema.wrap(out_schema);
        ArrowArray outArray = ArrowArray.wrap(out_array);

        this.transformedRoot = Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
        // System.out.println("java out vsr " + transformedRoot.contentToTSVString());

        System.out.println("finish");
        // JniWrapper.get().finish(instancePtr, context, schemaPtr, arrayPtr);
        this.context = context;
        this.schemaPtr = schemaPtr;
        this.arrayPtr = arrayPtr;
        // this.transformedRoot.close();
        System.out.println("next completed");
    }

    public void releaseHelpers() {
        // System.out.println("release helpers");
        JniWrapper.get().finish(instancePtr, context, schemaPtr, arrayPtr);
        this.transformedRoot.close();
    }

    public void close() throws Exception {
        if (!this.closed) {
            // System.out.println("close wasm transformer");
            if (this.transformedRoot != null) {
                this.transformedRoot.close();
            }
            this.closed = true;
        }
    }
}
