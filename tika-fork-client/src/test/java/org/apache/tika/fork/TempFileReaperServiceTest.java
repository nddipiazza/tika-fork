package org.apache.tika.fork;

import org.apache.commons.io.FileUtils;
import org.apache.tika.client.TempFileReaperService;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class TempFileReaperServiceTest {
  private static final Logger LOG = LoggerFactory.getLogger(TempFileReaperServiceTest.class);

  @Test
  public void testDeleteOldFiles() throws Exception {
    File curDir = new File("build", "test_working_dir");
    curDir.mkdirs();
    TempFileReaperService tempFileReaperService = null;
    try {
      File theFile1 = new File(curDir, "newFile1.txt");
      FileUtils.writeStringToFile(theFile1, "hey there!", "UTF-8");
      File theFile2 = new File(curDir, "newFile2.txt");
      FileUtils.writeStringToFile(theFile2, "hey there!", "UTF-8");
      tempFileReaperService = new TempFileReaperService(curDir.getAbsolutePath(),
          30,
          ChronoUnit.SECONDS,
          0,
          3,
          TimeUnit.SECONDS);

      LOG.info("Running awaitility for up to 40 seconds");

      long startedOn = System.currentTimeMillis();

      Awaitility.await().atMost(40, TimeUnit.SECONDS).until(() -> !theFile1.exists() && !theFile2.exists());

      Assert.assertFalse(theFile1.exists());
      Assert.assertFalse(theFile2.exists());

    } finally {
      if (tempFileReaperService != null) {
        tempFileReaperService.close();
      }
      FileUtils.deleteQuietly(curDir);
    }
  }
}
