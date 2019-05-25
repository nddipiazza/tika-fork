package org.apache.tika.fork;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.utils.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TikaProcessTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessTest.class);

  String tikaDistPath;
  String javaPath = "java";
  int numThreads = 20;
  String pdfPath = "test-files" + File.separator + "pdf-sample.pdf";
  String htmlPath = "test-files" + File.separator + "html-sample.html";
  String xlsPath = "test-files" + File.separator + "xls-sample.xls";
  String bombFilePath = "test-files" + File.separator + "bomb.xls";
  String bombContentType = "application/vnd.ms-excel";

  private AssertionError exc;

  @Before
  public void init() throws Exception {
    tikaDistPath = ".." + File.separator + "main" + File.separator + "build" + File.separator + "dist";
  }

  @Test
  public void testExternalTikaMultiThreaded() throws Exception {
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      tikaDistPath,
      200,
      -1,
      -1,
      20,
      -1,
      -1)) {
      doMultiThreadedParse(tikaProcessPool);
    }
  }

  private void doMultiThreadedParse(TikaProcessPool tikaProcessPool) throws Exception {
    int numFiles = 50;
    AtomicInteger numParsed = new AtomicInteger(0);
    AtomicBoolean failTest = new AtomicBoolean(false);
    Runnable r = () -> {
      try {
        try {
          for (int i = 0; i < numFiles; ++i) {
            if (exc != null) {
              return;
            }
            String path;
            String contentType;
            int numExpectedMetadataElms;
            int numContentCharsExpected;
            if (i % 3 == 0) {
              path = xlsPath;
              contentType = "application/vnd.ms-excel";
              numExpectedMetadataElms = 23;
              numContentCharsExpected = 4824;
            } else if (i % 3 == 1) {
              path = pdfPath;
              contentType = "application/pdf";
              numExpectedMetadataElms = 39;
              numContentCharsExpected = 1069;
            } else {
              path = htmlPath;
              contentType = "text/html";
              numExpectedMetadataElms = 8;
              numContentCharsExpected = 2648;
            }
            ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
            try (FileInputStream fis = new FileInputStream(path)) {
              Metadata metadata = tikaProcessPool.parse(path, contentType, false, fis, contentOutputStream);
              LOG.info("Metadata from the tika process: {}", metadata);
              Assert.assertEquals(numExpectedMetadataElms, metadata.size());
              //LOG.info("Content from the tika process: {}", contentOutputStream.toString("UTF-8"));
              Assert.assertEquals(numContentCharsExpected, contentOutputStream.toString("UTF-8").length());
              numParsed.incrementAndGet();
            }
          }
        } catch (Exception ex) {
          Assert.fail("Unexpected exception: " + ExceptionUtils.getStackTrace(ex));
        }
      } catch (AssertionError ae) {
        exc = ae;
      }
    };
    List<Thread> ts = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      Thread t = new Thread(r);
      t.start();
      ts.add(t);
    }
    for (Thread t : ts) {
      t.join();
    }
    if (exc != null) {
      throw exc;
    }
    Assert.assertFalse("Unxpected exception happened during parse", failTest.get());
    Assert.assertEquals(numFiles * numThreads, numParsed.get());
  }

  @Test
  public void testExternalTikaBombSingleThread() throws Exception {
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      tikaDistPath,
      200,
      1,
      1,
      1,
      -1,
      -1)) {
      ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
      try (FileInputStream fis = new FileInputStream(bombFilePath)) {
        tikaProcessPool.parse(bombFilePath, bombContentType, false, fis, contentOutputStream);
        Assert.fail("Should have OOM'd");
      } catch (Exception e) {
        LOG.info("Got the expected exception", e);
      }
    }
  }
}