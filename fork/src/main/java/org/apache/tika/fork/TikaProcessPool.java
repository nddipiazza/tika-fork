package org.apache.tika.fork;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.InputStream;

public class TikaProcessPool implements AutoCloseable {
  private String tikaDistPath;
  private int numProcesses;
  private ObjectPool pool;

  public TikaProcessPool(String tikaDistPath, int numProcesses) throws Exception {
    this.numProcesses = numProcesses;
    this.tikaDistPath = tikaDistPath;

    //Create the Object pool.
    pool = initializePool(numProcesses, tikaDistPath);
  }

  @Override
  public void close() {
    pool.close();
  }

  public void parse(InputStream contentInputStream) {
    TikaProcess process;

    try {
      process = (TikaProcess) pool.borrowObject();
      try {
        process.parse(contentInputStream);
      } catch (Exception e) {
        // invalidate the object
        pool.invalidateObject(process);
        // do not return the object to the pool twice
        process = null;
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
  public static ObjectPool initializePool(int numMaxTotal, String tikaDistDir) throws Exception {
    // We confugure the pool using a GenericObjectPoolConfig
    //Note: In the default implementation of Object Pool, objects are not created at start-up, but rather are created whenever the first call
    //to the pool.borrowObject() is made. This object is then cached for future use.
    //It is recommended to put these settings in a properties file.
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(numMaxTotal);
    config.setMinIdle(numMaxTotal);
    config.setBlockWhenExhausted(true);

    //We use the GenericObjectPool implementation of Object Pool as this suffices for most needs.
    //When we create the object pool, we need to pass the Object Factory class that would be responsible for creating the objects.
    //Also pass the config to the pool while creation.
    ObjectPool pool = new GenericObjectPool<TikaProcess>(new TikaProcessFactory(tikaDistDir), config);

    return pool;
  }
}
