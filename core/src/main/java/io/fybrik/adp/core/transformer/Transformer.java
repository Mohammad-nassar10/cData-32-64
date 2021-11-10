package io.fybrik.adp.core.transformer;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface Transformer extends AutoCloseable {
    void init(VectorSchemaRoot root);

    VectorSchemaRoot root();

    void next();

    VectorSchemaRoot originalRoot();
}
