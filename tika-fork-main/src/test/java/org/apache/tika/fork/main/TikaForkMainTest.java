package org.apache.tika.fork.main;

import org.apache.tika.client.TikaRunner;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TikaForkMainTest {

  private static final Logger LOG = LoggerFactory.getLogger(TikaForkMainTest.class);

  String pdfPath = "test-files" + File.separator + "pdf-sample.pdf";
  String htmlPath = "test-files" + File.separator + "html-sample.html";
  String xlsPath = "test-files" + File.separator + "xls-sample.xls";
  String txtPath = "test-files" + File.separator + "out.txt";
  String bombFilePath = "test-files" + File.separator + "bomb.xls";
  String zipBombPath = "test-files" + File.separator + "zip-bomb.zip";
  String oneNoteFilePath = "test-files" + File.separator + "test-one-note.one";

  @Test
  public void testMaxBytes() throws Exception {
    String [] args = {
      "-workDirectoryPath",
      System.getProperty("java.io.tmpdir"),
      "-parserPropertiesFilePath",
      Paths.get("test-files", "parse.properties").toAbsolutePath().toString(),
      "-contentInServerPort",
      "9001",
      "-metadataOutServerPort",
      "9002",
      "-contentOutServerPort",
      "9003"
    };

    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(9001, 9002, 9003, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(bombFilePath)) {
      Metadata metadata = tikaRunner.parse(bombFilePath,
        "application/vnd.ms-excel",
        fis,
        contentOutputStream,
        300000L,
        500
      );

      LOG.info("Metadata {}", metadata);

      System.out.println(new String(contentOutputStream.toByteArray()));
    }

    singleThreadEx.shutdownNow();
  }
}
