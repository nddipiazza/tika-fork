package org.apache.tika.fork;

import org.apache.tika.metadata.Metadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TikaProcessTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessTest.class);

  String tikaDistPath;

  @Before
  public void init() throws Exception {
    tikaDistPath = ".." + File.separator + "main" + File.separator + "build" + File.separator + "dist";
  }

  @Test
  public void testExternalTika() throws Exception {
    AtomicBoolean failTest = new AtomicBoolean(false);
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(tikaDistPath, 1, 4, 4)) {
      Runnable r = () -> {
        try {
          int numFiles = 50;
          for (int i = 0; i < numFiles; ++i) {
            ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
            try (FileInputStream fis = new FileInputStream("test-files" + File.separator + "pdf-sample.pdf")) {
              Metadata metadata = tikaProcessPool.parse(fis, contentOutputStream);
              Assert.assertEquals(39, metadata.size());
              LOG.info("Metadata from the tika process: {}", metadata);
              LOG.info("Content from the tika process: {}", contentOutputStream.toString("UTF-8"));
              Assert.assertEquals(1069, contentOutputStream.toString("UTF-8").length());
            }
          }
        } catch (IOException e) {
          failTest.set(true);
        }
      };
      Thread t1 = new Thread(r);
      Thread t2 = new Thread(r);
      Thread t3 = new Thread(r);

      t1.start();
      t2.start();
      t3.start();

      t1.join();
      t2.join();
      t3.join();
    }
    Assert.assertFalse(failTest.get());
  }
}