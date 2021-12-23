package io.fybrik.adp.core.transformer;

import java.io.ByteArrayOutputStream;

class NoCopyByteArrayOutputStream extends ByteArrayOutputStream {
    public NoCopyByteArrayOutputStream() {
        super();
    }

    public NoCopyByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getBuffer() {
        return buf;
    }
}
