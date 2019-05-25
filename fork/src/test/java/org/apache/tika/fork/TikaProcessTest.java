package org.apache.tika.fork;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

public class TikaProcessTest {

  String tikaDistPath;

  @Before
  public void init() throws Exception {
    tikaDistPath = ".." + File.separator + "main" + File.separator + "build" + File.separator + "dist";
  }

  @Test
  public void testExternalTika() throws Exception {
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(tikaDistPath, 4)) {
      int numFiles = 50;
      for (int i = 0; i < numFiles; ++i) {
        try (FileInputStream fis = new FileInputStream("/home/ndipiazza/Downloads/pdf-sample.pdf")) {
          tikaProcessPool.parse(fis);
        }
      }
    }
  }
}