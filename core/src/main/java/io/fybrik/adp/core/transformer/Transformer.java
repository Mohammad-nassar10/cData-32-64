package io.fybrik.adp.core.transformer;

import java.io.IOException;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface Transformer extends AutoCloseable {
    void init(VectorSchemaRoot root);

    VectorSchemaRoot root();

    void next() throws IOException;

    VectorSchemaRoot originalRoot();

    void releaseHelpers();
}
