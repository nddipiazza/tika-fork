package org.apache.tika.client;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TikaRunner {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcess.class);

  private int contentInPort = 0;
  private int metadataOutPort = 0;
  private int contentOutPort = 0;
  private boolean parseContent;

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
                        InputStream contentInStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs,
                        long maxBytesToParse) throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    es.execute(() -> {
      try {
        writeContent(baseUri, contentType, contentInPort, contentInStream, maxBytesToParse);
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
        try {
          getContent(contentOutPort, contentOutputStream);
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
          LOG.debug("Still waiting for content from parse");
          if (Instant.now().isAfter(mustFinishByInstant)) {
            throw e;
          }
        }
      }
    }

    Metadata metadataResult;
    while (true) {
      try {
        metadataResult = metadataFuture.get(250, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException e) {
        LOG.debug("Still waiting for metadata from parse");
        if (Instant.now().isAfter(mustFinishByInstant)) {
          throw e;
        }
      }
    }

    es.shutdown();

    return metadataResult;
  }

  private void writeContent(String baseUri,
                            String contentType,
                            int port,
                            InputStream contentInStream,
                            long maxBytesToParse) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (OutputStream out = socket.getOutputStream();
         BoundedInputStream boundedInputStream = new BoundedInputStream(contentInStream, maxBytesToParse)) {
      out.write(baseUri.getBytes());
      out.write('\n');
      out.write(contentType.getBytes());
      out.write('\n');
      long numChars;
      do {
        numChars = IOUtils.copy(boundedInputStream, out);
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
      } catch (IOException e) {
        LOG.warn("Could not parse metadata for " + baseUri);
        return new Metadata();
      }
    } finally {
      socket.close();
    }
  }

  private void getContent(int port, OutputStream contentOutputStream) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (InputStream in = socket.getInputStream()) {
      long numChars;
      do {
        numChars = IOUtils.copy(in, contentOutputStream);
      } while (numChars > 0);
    } finally {
      socket.close();
    }
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
