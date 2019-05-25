package org.apache.tika.fork;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TikaProcessFactory extends BasePooledObjectFactory<TikaProcess> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessFactory.class);

  private String tikaDistPath;

  public TikaProcessFactory(String tikaDistPath) {
    this.tikaDistPath = tikaDistPath;
  }

  @Override
  public TikaProcess create() {
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
    p.getObject().close();
  }
}