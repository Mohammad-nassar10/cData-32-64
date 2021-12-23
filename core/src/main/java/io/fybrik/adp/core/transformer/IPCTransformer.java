package io.fybrik.adp.core.transformer;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.jni.JniWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

public class IPCTransformer implements Transformer {
    private final BufferAllocator allocator;
    private final long instancePtr;
    private VectorSchemaRoot originalRoot;
    private VectorSchemaRoot transformedRoot;
    private boolean closed;

  

    public IPCTransformer(BufferAllocator allocator, Instance instance) {
        this.allocator = allocator;
        this.instancePtr = instance.getInstancePtr();
    }
    
    public void init(VectorSchemaRoot root) {
        Preconditions.checkState(this.originalRoot == null, "init can only be called once");
        this.originalRoot = root;
        this.closed = false;
    }

    public VectorSchemaRoot originalRoot() {
        return this.originalRoot;
    }

    public VectorSchemaRoot root() {
        return this.transformedRoot;
    }

    private byte[] WASMTransformByteArray(long instance_ptr, byte[] recordBatchByteArray, int size) {
        // Allocate a block in wasm memory and copy the byte array to this block
        long allocatedAddress = JniWrapper.get().wasmAlloc(instance_ptr, size);
        long wasm_mem_address = JniWrapper.get().wasmMemPtr(instance_ptr);
        ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, size);
        buffer.put(recordBatchByteArray, 0, size);
        // System.out.println("size = " + size);

        // Transform the vector schema root that is represented as a byte array in
        // `allocatedAddress` memory address with length `size`
        // The function returns a tuple of `(address, lenght)` of as byte array that
        // represents the transformed vector schema root
        long transformed_bytes_tuple = JniWrapper.get().TransformationIPC(instance_ptr, allocatedAddress, size);

        // Get the byte array from the memory address
        long transformed_bytes_address = JniWrapper.get().GetFirstElemOfTuple(instance_ptr, transformed_bytes_tuple);
        long transformed_bytes_len = JniWrapper.get().GetSecondElemOfTuple(instance_ptr, transformed_bytes_tuple);
        wasm_mem_address = JniWrapper.get().wasmMemPtr(instance_ptr);
        ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address + wasm_mem_address,
                (int) transformed_bytes_len);
        byte[] transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
        transformed_buffer.get(transformedRecordBatchByteArray);
        // System.out.println("transformed byte array = " + transformed_bytes_address);
        // Deallocate transformed bytes
        // JniWrapper.get().wasmDealloc(instance_ptr, transformed_bytes_address, transformed_bytes_len);
        JniWrapper.get().DropTuple(instance_ptr, transformed_bytes_tuple);
        return transformedRecordBatchByteArray;
    }

    public void next() throws IOException {
        // Create a contex with empty schema and array
        try (NoCopyByteArrayOutputStream out = new NoCopyByteArrayOutputStream()) {
            // Write the input vector schema root to the byte array output stream in IPC
            // format
            try (ArrowStreamWriter writer = new ArrowStreamWriter(originalRoot, null,
                    Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            // Transform the original record batch
            byte[] transformedRecordBatchByteArray = WASMTransformByteArray(instancePtr, out.getBuffer(),
                    out.size());

            // Read the byte array to get the transformed batch
            ArrowStreamReader reader = new ArrowStreamReader(
                    new ByteArrayInputStream(transformedRecordBatchByteArray), allocator);
                transformedRoot = reader.getVectorSchemaRoot();
                
                // VectorSchemaRoot transformedVSR = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                // this.transformedRoot = transformedVSR;
                // transformedVSR.close();
                // this.transformedRoot.close();
                // System.out.println("java out vsr " + this.transformedRoot.contentToTSVString());
                // VectorUnloader unloader = new VectorUnloader(transformedVSR);

                // // First time initialization
                // if (loader == null) {
                //     VectorSchemaRoot root_out = VectorSchemaRoot.create(transformedVSR.getSchema(), allocator);
                //     loader = new VectorLoader(root_out);
                //     listener.setUseZeroCopy(zeroCopy);
                //     listener.start(root_out);
                // }
                // loader.load(unloader.getRecordBatch());
                // listener.putNext();
            
        }
    // listener.completed();

        System.out.println("finish");

        // this.transformedRoot.close();
        System.out.println("next completed");
    }

    public void close() throws Exception {
        System.out.println("close wasm transformer1");
        if (!this.closed) {
            System.out.println("close wasm transformer2");
            if (this.transformedRoot != null) {
                System.out.println("close wasm transformer3");
                this.transformedRoot.close();
            }
            this.closed = true;
        }
    }

    @Override
    public void releaseHelpers() {
        this.transformedRoot.close();
    }
}
