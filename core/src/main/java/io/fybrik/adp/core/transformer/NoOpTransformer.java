package io.fybrik.adp.core.transformer;

import org.apache.arrow.vector.VectorSchemaRoot;

public class NoOpTransformer implements Transformer {
    VectorSchemaRoot root;

    public NoOpTransformer() {
    }

    public void close() throws Exception {
    }

    public void init(VectorSchemaRoot root) {
        this.root = root;
    }

    public VectorSchemaRoot root() {
        return this.root;
    }

    public void next() {
    }

    @Override
    public VectorSchemaRoot originalRoot() {
        // TODO Auto-generated method stub
        return this.root;
    }
}
