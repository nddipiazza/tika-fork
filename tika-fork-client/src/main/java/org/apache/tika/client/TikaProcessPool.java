package org.apache.tika.client;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class TikaProcessPool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessPool.class);

  private GenericObjectPool pool;

  public TikaProcessPool(String javaPath,
                         String configDirectoryPath,
                         String tikaDistPath,
                         int tikaMaxHeapSizeMb,
                         Properties parseProperties,
                         int numMinIdle,
                         int numMaxIdle,
                         int numMaxTotal,
                         boolean blockWhenExhausted,
                         long maxWaitMillis,
                         long timeBetweenEvictionRunsMillis,
                         long minEvictableIdleTimeMillis,
                         long softMinEvictableIdleTimeMillis) throws Exception {
    pool = initializePool(javaPath,
        configDirectoryPath,
        tikaDistPath,
        tikaMaxHeapSizeMb,
        parseProperties,
        numMinIdle,
        numMaxIdle,
        numMaxTotal,
        blockWhenExhausted,
        maxWaitMillis,
        timeBetweenEvictionRunsMillis,
        minEvictableIdleTimeMillis,
        softMinEvictableIdleTimeMillis);
  }

  @Override
  public void close() {
    pool.close();
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        InputStream contentInputStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs) throws Exception {
    TikaProcess process = (TikaProcess) pool.borrowObject();
    try {
      return process.parse(baseUri, contentType, contentInputStream, contentOutputStream, abortAfterMs);
    } catch (Exception e) {
      pool.invalidateObject(process);
      // Do not return the object to the pool twice
      process = null;
      throw e;
    } finally {
      // Make sure the object is returned to the pool
      if (null != process) {
        pool.returnObject(process);
      }
    }
  }

  public static GenericObjectPool initializePool(String javaPath,
                                                 String configDirectoryPath,
                                                 String tikaDistDir,
                                                 int tikaMaxHeapSizeMb,
                                                 Properties parseProperties,
                                                 int numMinIdle,
                                                 int numMaxIdle,
                                                 int numMaxTotal,
                                                 boolean blockWhenExhausted,
                                                 long maxWaitMillis,
                                                 long timeBetweenEvictionRunsMillis,
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
    config.setMaxWaitMillis(maxWaitMillis);
    config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    config.setBlockWhenExhausted(blockWhenExhausted);

    GenericObjectPool pool = new GenericObjectPool<TikaProcess>(new TikaProcessFactory(javaPath,
        configDirectoryPath,
        tikaDistDir,
        tikaMaxHeapSizeMb,
        parseProperties), config);

    return pool;
  }
}
