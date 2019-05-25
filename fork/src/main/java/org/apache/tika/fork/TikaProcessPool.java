package org.apache.tika.fork;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;

public class TikaProcessPool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessPool.class);

  private ObjectPool pool;

  public TikaProcessPool(String tikaDistPath,
                         int tikaMaxHeapSizeMb,
                         int numMinIdle,
                         int numMaxIdle,
                         int numMaxTotal,
                         long minEvictableIdleTimeMillis,
                         long softMinEvictableIdleTimeMillis) throws Exception {
    pool = initializePool(tikaDistPath, tikaMaxHeapSizeMb, numMinIdle, numMaxIdle, numMaxTotal, minEvictableIdleTimeMillis, softMinEvictableIdleTimeMillis);
  }

  @Override
  public void close() {
    pool.close();
  }

  public Metadata parse(String baseUri, String contentType, boolean extractHtmlLinks, InputStream contentInputStream, OutputStream contentOutputStream) {
    TikaProcess process;

    try {
      process = (TikaProcess) pool.borrowObject();
      try {
        return process.parse(baseUri, contentType, extractHtmlLinks, contentInputStream, contentOutputStream);
      } catch (Exception e) {
        pool.invalidateObject(process);
        // Do not return the object to the pool twice
        process = null;
        throw new RuntimeException("Could not parse content input stream", e);
      } finally {
        // Make sure the object is returned to the pool
        if (null != process) {
          pool.returnObject(process);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to borrow TikaProcess from the pool", e);
    }
  }

  public static ObjectPool initializePool(String tikaDistDir,
                                          int tikaMaxHeapSizeMb,
                                          int numMinIdle,
                                          int numMaxIdle,
                                          int numMaxTotal,
                                          long minEvictableIdleTimeMillis,
                                          long softMinEvictableIdleTimeMillis) throws Exception {
    // Note: In the default implementation of Object Pool, objects are not created at start-up, but rather are created whenever the first call
    // to the pool.borrowObject() is made. This object is then cached for future use.
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMinIdle(numMinIdle);
    config.setMaxIdle(numMaxIdle);
    config.setMaxTotal(numMaxTotal);
    config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    config.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    config.setBlockWhenExhausted(true);

    ObjectPool pool = new GenericObjectPool<TikaProcess>(new TikaProcessFactory(tikaDistDir, tikaMaxHeapSizeMb), config);

    return pool;
  }
}
