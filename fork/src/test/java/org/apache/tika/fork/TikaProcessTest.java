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
import java.util.ArrayList;
import java.util.List;
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
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(tikaDistPath,
      200,
      1,
      10,
      10,
      -1,
      -1)) {
      Runnable r = () -> {
        try {
          int numFiles = 100;
          for (int i = 0; i < numFiles; ++i) {
            ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
            String path = "test-files" + File.separator + "pdf-sample.pdf";
            String contentType = "application/pdf";
            try (FileInputStream fis = new FileInputStream(path)) {
              Metadata metadata = tikaProcessPool.parse(path, contentType, false, fis, contentOutputStream);
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
      List<Thread> ts = new ArrayList<>();
      for (int i=0; i<10; ++i) {
        Thread t = new Thread(r);
        t.start();
        ts.add(t);
      }
      for (Thread t : ts) {
        t.join();
      }
    }
    Assert.assertFalse(failTest.get());
  }
}