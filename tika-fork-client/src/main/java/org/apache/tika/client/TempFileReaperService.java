package org.apache.tika.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.io.File;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A service that cleans up temp files from the work dir.
 */
public class TempFileReaperService {

  private ScheduledExecutorService scheduledExecutorService;

  /**
   * Create and run the service.
   *
   * @param workDirectoryPath             Work path where files are deleted from
   * @param tempFileReaperMaxFileAge      how old can a file be?
   * @param tempFileReaperMaxFileAgeUnit  time unit of the file's age
   * @param tempFileReaperJobInitialDelay number of initial job delay time units
   * @param tempFileReaperJobDelay        number of job delay time units
   * @param tempFileReaperJobDelayUnit    the job delay time unit type
   */
  public TempFileReaperService(String workDirectoryPath,
                               long tempFileReaperMaxFileAge,
                               TemporalUnit tempFileReaperMaxFileAgeUnit,
                               long tempFileReaperJobInitialDelay,
                               long tempFileReaperJobDelay,
                               TimeUnit tempFileReaperJobDelayUnit) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleWithFixedDelay(
        () -> {
          try {
            OffsetDateTime now = OffsetDateTime.now(ZoneId.systemDefault());
            OffsetDateTime oldestTimeAllowed = now.minus(Duration.of(tempFileReaperMaxFileAge, tempFileReaperMaxFileAgeUnit));

            Date threshold = Date.from(oldestTimeAllowed.toInstant());
            AgeFileFilter filter = new AgeFileFilter(threshold);

            File path = new File(workDirectoryPath);
            File[] oldFiles = FileFilterUtils.filter(filter, path);

            for (File file : oldFiles) {
              FileUtils.deleteQuietly(file);
            }
          } catch (Exception e) {
            throw new RuntimeException("Could not run the schedule", e);
          }
        },
        tempFileReaperJobInitialDelay,
        tempFileReaperJobDelay,
        tempFileReaperJobDelayUnit);
  }

  /**
   * Shut down the executor, cancelling any pending jobs.
   */
  public void close() {
    if (!scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdownNow();
    }
  }
}
