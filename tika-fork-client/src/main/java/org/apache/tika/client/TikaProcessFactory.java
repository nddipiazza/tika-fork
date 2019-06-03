package org.apache.tika.client;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TikaProcessFactory extends BasePooledObjectFactory<TikaProcess> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessFactory.class);

  private String tikaDistPath;
  private String javaPath;
  private String configDirectoryPath;
  private int tikaMaxHeapSizeMb;
  private Properties parseProperties;

  public TikaProcessFactory(String javaPath, String configDirectoryPath, String tikaDistPath, int tikaMaxHeapSizeMb, Properties parseProperties) {
    this.tikaDistPath = tikaDistPath;
    this.tikaMaxHeapSizeMb = tikaMaxHeapSizeMb;
    this.javaPath = javaPath;
    this.parseProperties = parseProperties;
    this.configDirectoryPath = configDirectoryPath;
  }

  @Override
  public TikaProcess create() {
    return new TikaProcess(javaPath, configDirectoryPath, tikaDistPath, tikaMaxHeapSizeMb, parseProperties);
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