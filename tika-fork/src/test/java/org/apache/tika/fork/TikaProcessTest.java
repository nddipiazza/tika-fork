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
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TikaProcessTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessTest.class);

  String tikaDistPath;
  String javaPath = "java";
  int numThreads;
  int numFilesPerThread;
  String pdfPath = "test-files" + File.separator + "pdf-sample.pdf";
  String htmlPath = "test-files" + File.separator + "html-sample.html";
  String xlsPath = "test-files" + File.separator + "xls-sample.xls";
  String bombFilePath = "test-files" + File.separator + "bomb.xls";
  String bombContentType = "application/vnd.ms-excel";
  Properties parseProperties;

  AssertionError exc;

  @Before
  public void init() {
    tikaDistPath = ".." + File.separator + "tika-fork-main" + File.separator + "build" + File.separator + "dist";
    parseProperties = new Properties();
    parseProperties.setProperty("parseContent", "true");
  }

  @Test
  public void testExternalTikaMultiThreaded() throws Exception {
    numThreads = 5;
    numFilesPerThread = 50;
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      System.getProperty("java.io.tmpdir"),
      tikaDistPath,
      200,
      parseProperties,
      -1,
      -1,
      20,
      -1,
      -1)) {
      doParse(tikaProcessPool, true);
    }
  }

  @Test
  public void testExternalTikaSingleThreaded() throws Exception {
    numThreads = 1;
    numFilesPerThread = 50;
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      System.getProperty("java.io.tmpdir"),
      tikaDistPath,
      200,
      parseProperties,
      -1,
      -1,
      20,
      -1,
      -1)) {
      doParse(tikaProcessPool, true);
    }
  }

  @Test
  public void testExternalTikaSingleNoContent() throws Exception {
    numThreads = 5;
    numFilesPerThread = 50;
    parseProperties.setProperty("parseContent", "false");
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      System.getProperty("java.io.tmpdir"),
      tikaDistPath,
      200,
      parseProperties,
      -1,
      -1,
      20,
      -1,
      -1)) {
      doParse(tikaProcessPool, false);
    }
  }

  private void doParse(TikaProcessPool tikaProcessPool, boolean parseContent) throws Exception {
    AtomicInteger numParsed = new AtomicInteger(0);
    Runnable r = () -> {
      try {
        try {
          for (int i = 0; i < numFilesPerThread; ++i) {
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
              numContentCharsExpected = parseContent ? 4824 : 0;
            } else if (i % 3 == 1) {
              path = pdfPath;
              contentType = "application/pdf";
              numExpectedMetadataElms = 39;
              numContentCharsExpected = parseContent ? 1069 : 0;
            } else {
              path = htmlPath;
              contentType = "text/html";
              numExpectedMetadataElms = 8;
              numContentCharsExpected = parseContent ? 2648 : 0;
            }
            ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
            try (FileInputStream fis = new FileInputStream(path)) {
              Metadata metadata = tikaProcessPool.parse(path,
                contentType,
                fis,
                contentOutputStream,
                300000L);
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
    Assert.assertEquals(numFilesPerThread * numThreads, numParsed.get());
  }

  @Test
  public void testExternalTikaBombSingleThread() throws Exception {
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      System.getProperty("java.io.tmpdir"),
      tikaDistPath,
      200,
      parseProperties,
      1,
      1,
      1,
      -1,
      -1)) {
      ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
      try (FileInputStream fis = new FileInputStream(bombFilePath)) {
        tikaProcessPool.parse(bombFilePath, bombContentType, fis, contentOutputStream, 300000L);
        Assert.fail("Should have OOM'd");
      } catch (Exception e) {
        LOG.info("Got the expected exception", e);
      }
    }
  }

  @Test
  public void testTikaParseTimeoutExceeded() throws Exception {
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
      System.getProperty("java.io.tmpdir"),
      tikaDistPath,
      200,
      parseProperties,
      1,
      1,
      1,
      -1,
      -1)) {
      ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
      try (FileInputStream fis = new FileInputStream(bombFilePath)) {
        tikaProcessPool.parse(bombFilePath, bombContentType, fis, contentOutputStream, 5000L);
        Assert.fail("Should have timed out");
      } catch (TimeoutException e) {
        LOG.info("Got the expected exception", e);
      }
    }
  }
}