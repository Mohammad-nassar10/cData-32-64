package io.fybrik.adp.core.jni;

/**
 * JniWrapper for ADP core.
 */
public class JniWrapper {
  private static final JniWrapper INSTANCE = new JniWrapper();

  public static JniWrapper get() {
    return INSTANCE;
  }

  private JniWrapper() {
    JniLoader.get().ensureLoaded();
  }

  public native long newInstance(byte[] moduleBytes);

  public native void dropInstance(long instancePtr);

  public native long prepare(long instancePtr);

  public native long transform(long instancePtr, long contextPtr);

  public native void finish(long instancePtr, long contextPtr);

  public native long getInputSchema(long instancePtr, long contextPtr);

  public native long getInputArray(long instancePtr, long contextPtr);

  public native long getOutputSchema(long contextPtr);

  public native long getOutputArray(long contextPtr);

  public native long wasmAlloc(long instancePtr, long size);

  public native long wasmMemPtr(long instancePtr);

  public native void wasmDealloc(long instancePtr, long offset, long size);

  public native long allocatedSize(long instancePtr);

  public native long releasedSize(long instancePtr);

  // public native void convert_to_32(long instancePtr, long contextPtr);

}