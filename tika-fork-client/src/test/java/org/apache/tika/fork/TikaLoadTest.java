package org.apache.tika.fork;

import org.apache.tika.client.TikaProcessPool;
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
import java.net.FileNameMap;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TikaLoadTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaLoadTest.class);

  String tikaDistPath;
  String javaPath = "java";
  int numThreads;
  String corpaPath;

  Properties parseProperties;
  long maxBytesToParse = 256000000;

  AssertionError exc;

  @Before
  public void init() {
    tikaDistPath = ".." + File.separator + "tika-fork-main" + File.separator + "build" + File.separator + "dist";
    parseProperties = new Properties();
    parseProperties.setProperty("parseContent", "true");
    corpaPath = System.getProperty("corpaPath");
  }

  @Test
  public void testExternalTikaMultiThreaded() throws Exception {
    numThreads = 5;
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
        System.getProperty("java.io.tmpdir"),
        tikaDistPath,
        200,
        parseProperties,
        -1,
        -1,
        20,
        true,
        30000,
        3000,
        -1,
        -1)) {
      doParse(tikaProcessPool, true);
    }
  }

  private void doParse(TikaProcessPool tikaProcessPool, boolean parseContent) throws Exception {
    AtomicInteger numParsed = new AtomicInteger(0);
    Runnable r = () -> {
      try {
        try {
          for (File nextCorpaDir : new File(corpaPath).listFiles()) {
            if (nextCorpaDir.isDirectory()) {
              for (File nextCorpaFile : nextCorpaDir.listFiles()) {
                ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
                try (FileInputStream fis = new FileInputStream(nextCorpaFile)) {
                  FileNameMap fileNameMap = URLConnection.getFileNameMap();
                  String mimeType = fileNameMap.getContentTypeFor(nextCorpaFile.getName());
                  Metadata metadata = tikaProcessPool.parse(nextCorpaFile.getAbsolutePath(),
                      mimeType,
                      fis,
                      contentOutputStream,
                      300000L,
                      maxBytesToParse
                  );
                  LOG.info("Metadata: {}", metadata);
                  numParsed.incrementAndGet();
                } catch (Exception e) {
                  LOG.error("Failed attempt", e);
                }
              }
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
  }

}