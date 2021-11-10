package io.fybrik.adp.core.jni;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The JniLoader for C Data Interface API's native implementation.
 */
public class JniLoader {
  private static final JniLoader INSTANCE = new JniLoader(Collections.singletonList("adp_core"));

  public static JniLoader get() {
    return INSTANCE;
  }

  private final Set<String> librariesToLoad;

  private JniLoader(List<String> libraryNames) {
    librariesToLoad = new HashSet<>(libraryNames);
  }

  private boolean finished() {
    return librariesToLoad.isEmpty();
  }

  /**
   * If required JNI libraries are not loaded, then load them.
   */
  public void ensureLoaded() {
    if (finished()) {
      return;
    }
    loadRemaining();
  }

  private synchronized void loadRemaining() {
    // The method is protected by a mutex via synchronized, if more than one thread
    // race to call
    // loadRemaining, at same time only one will do the actual loading and the
    // others will wait for
    // the mutex to be acquired then check on the remaining list: if there are
    // libraries that were not
    // successfully loaded then the mutex owner will try to load them again.
    if (finished()) {
      return;
    }
    List<String> libs = new ArrayList<>(librariesToLoad);
    for (String lib : libs) {
      load(lib);
      librariesToLoad.remove(lib);
    }
  }

  private void load(String name) {
    System.out.println(name);
    if (name.equalsIgnoreCase("adp_core")) {
      System.load("/home/mohammadtn/FFI32/arrow-data-plane/core/src/main/rust/adp_core/target/debug/libadp_core.so");
      return;
    }
    final String libraryToLoad = System.mapLibraryName(name);
    try {
      File temp = File.createTempFile("jnilib-", ".tmp", new File(System.getProperty("java.io.tmpdir")));
      try (final InputStream is = JniWrapper.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
        if (is == null) {
          throw new FileNotFoundException(libraryToLoad);
        }
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.load(temp.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new IllegalStateException("error loading native libraries: " + e);
    }
  }
}