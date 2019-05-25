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

  public TikaProcessPool(String tikaDistPath, int numMinIdle, int numMaxIdle, int numMaxTotal) throws Exception {
    pool = initializePool(numMinIdle, numMaxIdle, numMaxTotal, tikaDistPath);
  }

  @Override
  public void close() {
    pool.close();
  }

  public Metadata parse(InputStream contentInputStream, OutputStream contentOutputStream) {
    TikaProcess process;

    try {
      process = (TikaProcess) pool.borrowObject();
      try {
        return process.parse(contentInputStream, contentOutputStream);
      } catch (Exception e) {
        // invalidate the object
        pool.invalidateObject(process);
        // do not return the object to the pool twice
        process = null;
        throw new RuntimeException("Could not parse content input stream", e);
      } finally {
        // make sure the object is returned to the pool
        if (null != process) {
          pool.returnObject(process);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to borrow TikaProcess from the pool", e);
    }
  }

  //A helper method to initialize the pool using the config and object-factory.
  public static ObjectPool initializePool(int numMinIdle, int numMaxIdle, int numMaxTotal, String tikaDistDir) throws Exception {
    // We configure the pool using a GenericObjectPoolConfig
    //Note: In the default implementation of Object Pool, objects are not created at start-up, but rather are created whenever the first call
    //to the pool.borrowObject() is made. This object is then cached for future use.
    //It is recommended to put these settings in a properties file.
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMinIdle(numMinIdle);
    config.setMaxIdle(numMaxIdle);
    config.setMaxTotal(numMaxTotal);
    config.setBlockWhenExhausted(true);

    //We use the GenericObjectPool implementation of Object Pool as this suffices for most needs.
    //When we create the object pool, we need to pass the Object Factory class that would be responsible for creating the objects.
    //Also pass the config to the pool while creation.
    ObjectPool pool = new GenericObjectPool<TikaProcess>(new TikaProcessFactory(tikaDistDir), config);

    return pool;
  }
}
