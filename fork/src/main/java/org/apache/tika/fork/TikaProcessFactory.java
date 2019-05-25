package org.apache.tika.fork;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

public class TikaProcessFactory extends BasePooledObjectFactory<TikaProcess> {

  private String tikaDistPath;

  public TikaProcessFactory(String tikaDistPath) {
    this.tikaDistPath = tikaDistPath;
  }

  @Override
  public TikaProcess create() {
    //Put in your logic for creating your expensive object - e.g. JDBC Connection, MQTT Connection, etc.
    //here for the sake of simplicity, we return a custom pooled object of a custom class called MyObject.
    try {
      return new TikaProcess(tikaDistPath);
    } catch (IOException e) {
      throw new RuntimeException("Could not create tika process", e);
    }
  }

  /**
   * Use the default PooledObject implementation. This helps in acting like a proxy for doing extra operations.
   * Please read API docs of DefaultPooledObject for more information
   */
  @Override
  public PooledObject<TikaProcess> wrap(TikaProcess myObject) {
    return new DefaultPooledObject<>(myObject);
  }

  @Override
  public void destroyObject(PooledObject<TikaProcess> p) {
    //Destroys an instance no longer needed by the pool.
    System.out.println("destroying");
    p.getObject().close();
  }

  // for all other methods, the no-op implementation
  // in BasePooledObjectFactory will suffice
}