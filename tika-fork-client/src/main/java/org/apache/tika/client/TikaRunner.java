package org.apache.tika.client;

import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TikaRunner {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcess.class);

  private int contentInPort = 0;
  private int metadataOutPort = 0;
  private int contentOutPort = 0;
  private boolean parseContent;
  private int contentChunkSize = 5000000;

  class TikaRunnerThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      return new Thread(r, "tikarunner");
    }
  }

  public TikaRunner(int contentInPort,
                    int metadataOutPort,
                    int contentOutPort,
                    boolean parseContent) {
    this.contentInPort = contentInPort;
    this.metadataOutPort = metadataOutPort;
    this.contentOutPort = contentOutPort;
    this.parseContent = parseContent;
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        String filename,
                        OutputStream contentOutputStream,
                        long abortAfterMs,
                        long maxBytesToParse) throws InterruptedException, ExecutionException, TimeoutException, IOException {
    try (FileInputStream fis = new FileInputStream(filename)) {
      return parse(baseUri,
          contentType,
          fis,
          contentOutputStream,
          abortAfterMs,
          maxBytesToParse);
    }
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        InputStream contentInStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs,
                        long maxBytesToParse) throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService es = Executors.newFixedThreadPool(3, new TikaRunnerThreadFactory());
    try {
      es.submit(() -> {
        try {
          writeContent(baseUri, contentType, contentInPort, contentInStream);
        } catch (Exception e) {
          throw new RuntimeException("Failed to send content stream to forked Tika parser JVM", e);
        }
      });
      Future<Metadata> metadataFuture = es.submit(() -> {
        try {
          return getMetadata(metadataOutPort, baseUri);
        } catch (Exception e) {
          throw new RuntimeException("Failed to read metadata from forked Tika parser JVM", e);
        }
      });

      Instant mustFinishByInstant = Instant.now().plus(Duration.ofMillis(abortAfterMs));
      if (parseContent) {
        Future contentFuture = es.submit(() -> {
          try (TikaRunnerGetContentResult contentResult = getContent(contentOutPort, contentOutputStream, maxBytesToParse, baseUri)) {
            while (!metadataFuture.isDone()) {
              if (Instant.now().isAfter(mustFinishByInstant)) {
                throw new TimeoutException("Timed out waiting " + abortAfterMs + " ms for metadata after content was fully parsed.");
              }
              Thread.sleep(100L);
            }
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to read content from forked Tika parser JVM", e);
          }
        });

        while (true) {
          try {
            contentFuture.get(250, TimeUnit.MILLISECONDS);
            break;
          } catch (TimeoutException e) {
            if (Instant.now().isAfter(mustFinishByInstant)) {
              throw e;
            }
            LOG.debug("Still waiting for content from parse");
          }
        }
      }

      Metadata metadataResult;
      while (true) {
        try {
          metadataResult = metadataFuture.get(250, TimeUnit.MILLISECONDS);
          break;
        } catch (TimeoutException e) {
          if (Instant.now().isAfter(mustFinishByInstant)) {
            throw e;
          }
          LOG.debug("Still waiting for metadata from parse");
        }
      }
      es.shutdown();
      return metadataResult;
    } finally {
      if (!es.isShutdown()) {
        es.shutdownNow();
      }
    }
  }

  private void writeContent(String baseUri,
                            String contentType,
                            int port,
                            InputStream contentInStream) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (OutputStream out = socket.getOutputStream()) {
      out.write(baseUri.getBytes());
      out.write('\n');
      out.write(contentType.getBytes());
      out.write('\n');
      long numChars;
      do {
        numChars = IOUtils.copy(contentInStream, out);
      } while (numChars > 0);
    } finally {
      socket.close();
    }
  }

  private Metadata getMetadata(int port, String baseUri) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (InputStream metadataIn = socket.getInputStream()) {
      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
      try {
        return (Metadata) objectInputStream.readObject();
      } catch (EOFException e) {
        // Is there some particular IOExceptions we should allow not to fall through?
        LOG.warn("Could not parse metadata for {} due to EOFException.", baseUri);
        Metadata blankedResult = new Metadata();
        blankedResult.set("eof_during_parse", "true");
        return blankedResult;
      }
    } finally {
      socket.close();
    }
  }

  static private class TikaRunnerGetContentResult implements AutoCloseable {
    public Socket socket;
    public InputStream inputStream;
    public boolean exceededMaxBytes;

    @Override
    public void close() {
      try {
        inputStream.close();
      } catch (IOException e) {
        LOG.error("Could not close GetContent input stream", e);
      }
      try {
        socket.close();
      } catch (IOException e) {
        LOG.error("Could not close GetContent socket", e);
      }
    }
  }

  private TikaRunnerGetContentResult getContent(int port, OutputStream contentOutputStream, long maxBytesToParse, String baseUri) throws Exception {
    TikaRunnerGetContentResult result = new TikaRunnerGetContentResult();
    result.socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    result.inputStream = result.socket.getInputStream();
    int numParsed = 0;
    int numChars;
    do {
      int nextNumBytesToParse = (numParsed + contentChunkSize > (int)maxBytesToParse) ?
        ((int)maxBytesToParse - numParsed) : contentChunkSize;
      byte[] buf = new byte[nextNumBytesToParse];
      numChars = IOUtils.read(result.inputStream, buf, 0, nextNumBytesToParse);
      contentOutputStream.write(buf, 0, numChars);
      numParsed += numChars;
      if (numParsed >= maxBytesToParse) {
        LOG.info("Max bytes {} reached on {}. Ignoring the rest.", maxBytesToParse, baseUri);
        result.exceededMaxBytes = true;
        return result;
      }
    } while (numChars > 0);
    return result;
  }

  private static Socket getSocket(String host, int port) throws InterruptedException {
    Socket socket;
    int maxRetries = 20;

    while (true) {
      try {
        socket = new Socket(host, port);
        if (socket != null || --maxRetries < 0) {
          break;
        }
      } catch (IOException e) {
        Thread.sleep(1000);
      }
    }

    return socket;
  }
}
