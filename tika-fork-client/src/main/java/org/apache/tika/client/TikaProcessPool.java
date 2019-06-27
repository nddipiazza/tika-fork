package org.apache.tika.client;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TikaProcessPool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcessPool.class);
  private static final TemporalUnit DEFAULT_TEMP_REAPER_UNIT = ChronoUnit.HOURS;
  private static final long DEFAULT_TEMP_REAPER_VAL = 2;
  private static final TimeUnit DEFAULT_TEMP_REAPER_JOB_UNIT = TimeUnit.MINUTES;
  private static final long DEFAULT_TEMP_REAPER_JOB_INITIAL_DELAY = 5;
  private static final long DEFAULT_TEMP_REAPER_JOB_DELAY = 10;

  private GenericObjectPool pool;
  private TempFileReaperService tempFileReaperService;

  public TikaProcessPool(String javaPath,
                         String workDirectoryPath,
                         String tikaDistPath,
                         int tikaMaxHeapSizeMb,
                         Properties parseProperties,
                         int numMinIdle,
                         int numMaxIdle,
                         int numMaxTotal,
                         boolean blockWhenExhausted,
                         long maxWaitMillis,
                         long timeBetweenEvictionRunsMillis,
                         long minEvictableIdleTimeMillis,
                         long softMinEvictableIdleTimeMillis) throws Exception {
    this(javaPath,
        workDirectoryPath,
        tikaDistPath,
        tikaMaxHeapSizeMb,
        parseProperties,
        numMinIdle,
        numMaxIdle,
        numMaxTotal,
        blockWhenExhausted,
        maxWaitMillis,
        timeBetweenEvictionRunsMillis,
        minEvictableIdleTimeMillis,
        softMinEvictableIdleTimeMillis,
        DEFAULT_TEMP_REAPER_VAL,
        DEFAULT_TEMP_REAPER_UNIT,
        DEFAULT_TEMP_REAPER_JOB_INITIAL_DELAY,
        DEFAULT_TEMP_REAPER_JOB_DELAY,
        DEFAULT_TEMP_REAPER_JOB_UNIT);
  }

  public TikaProcessPool(String javaPath,
                         String workDirectoryPath,
                         String tikaDistPath,
                         int tikaMaxHeapSizeMb,
                         Properties parseProperties,
                         int numMinIdle,
                         int numMaxIdle,
                         int numMaxTotal,
                         boolean blockWhenExhausted,
                         long maxWaitMillis,
                         long timeBetweenEvictionRunsMillis,
                         long minEvictableIdleTimeMillis,
                         long softMinEvictableIdleTimeMillis,
                         long tempFileReaperDelay,
                         TemporalUnit tempFileReaperDelayUnit,
                         long tempFileReaperJobInitialDelay,
                         long tempFileReaperJobDelay,
                         TimeUnit tempFileReaperJobDelayUnit) throws Exception {
    // Make sure the work directory exists.
    new File(workDirectoryPath).mkdirs();

    pool = initializePool(javaPath,
        workDirectoryPath,
        tikaDistPath,
        tikaMaxHeapSizeMb,
        parseProperties,
        numMinIdle,
        numMaxIdle,
        numMaxTotal,
        blockWhenExhausted,
        maxWaitMillis,
        timeBetweenEvictionRunsMillis,
        minEvictableIdleTimeMillis,
        softMinEvictableIdleTimeMillis);

    tempFileReaperService = new TempFileReaperService(workDirectoryPath,
        tempFileReaperDelay,
        tempFileReaperDelayUnit,
        tempFileReaperJobInitialDelay,
        tempFileReaperJobDelay,
        tempFileReaperJobDelayUnit);
  }

  @Override
  public void close() {
    pool.close();
    tempFileReaperService.close();
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        InputStream contentInputStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs,
                        long maxBytesToParse) throws Exception {
    TikaProcess process = (TikaProcess) pool.borrowObject();
    try {
      return process.parse(baseUri,
          contentType,
          contentInputStream,
          contentOutputStream,
          abortAfterMs,
          maxBytesToParse);
    } catch (Exception e) {
      pool.invalidateObject(process);
      // Do not return the object to the pool twice
      process = null;
      throw e;
    } finally {
      // Make sure the object is returned to the pool
      if (null != process) {
        pool.returnObject(process);
      }
    }
  }

  public static GenericObjectPool initializePool(String javaPath,
                                                 String workDirectoryPath,
                                                 String tikaDistDir,
                                                 int tikaMaxHeapSizeMb,
                                                 Properties parseProperties,
                                                 int numMinIdle,
                                                 int numMaxIdle,
                                                 int numMaxTotal,
                                                 boolean blockWhenExhausted,
                                                 long maxWaitMillis,
                                                 long timeBetweenEvictionRunsMillis,
                                                 long minEvictableIdleTimeMillis,
                                                 long softMinEvictableIdleTimeMillis) throws Exception {
    // Note: In the default implementation of Object Pool, objects are not created at start-up, but rather are created whenever the first call
    // to the pool.borrowObject() is made. This object is then cached for future use.
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMinIdle(numMinIdle);
    config.setMaxIdle(numMaxIdle);
    config.setMaxTotal(numMaxTotal);
    config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    config.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    config.setMaxWaitMillis(maxWaitMillis);
    config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    config.setBlockWhenExhausted(blockWhenExhausted);

    GenericObjectPool pool = new GenericObjectPool<TikaProcess>(new TikaProcessFactory(javaPath,
        workDirectoryPath,
        tikaDistDir,
        tikaMaxHeapSizeMb,
        parseProperties), config);

    return pool;
  }
}
