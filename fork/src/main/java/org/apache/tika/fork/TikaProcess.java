package org.apache.tika.fork;

import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TikaProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcess.class);

  static private final String CURRENT_JAVA_BINARY;

  static {
    if (System.getProperty("os.name").startsWith("Win")) {
      CURRENT_JAVA_BINARY = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java.exe";
    } else {
      CURRENT_JAVA_BINARY = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    }
  }

  private int contentInPort;
  private int metadataOutPort;
  private int contentOutPort;
  private Process process;
  private List<String> command;
  private String runUuid = UUID.randomUUID().toString();
  private String propertiesFilePath;
  private String portsFilePath;

  public TikaProcess(String javaPath, String tikaDistPath, int tikaMaxHeapSizeMb) throws IOException {
    command = new ArrayList<>();
    command.add(javaPath == null || javaPath.trim().length() == 0 ? CURRENT_JAVA_BINARY : javaPath);
    if (tikaMaxHeapSizeMb > 0) {
      command.add("-Xmx" + tikaMaxHeapSizeMb + "m");
    }
    command.add("-cp");
    command.add(tikaDistPath + File.separator + "*");
    command.add("org.apache.tika.main.TikaMain");
    command.add(runUuid);
    String parentFolder = System.getProperty("java.io.tmpdir") + File.separator;
    propertiesFilePath = parentFolder + "tika-fork-" + runUuid + ".properties";
    portsFilePath = parentFolder + "tika-ports-" + runUuid + ".properties";
    try {
      process = new ProcessBuilder(command)
        .inheritIO()
        .start();
      LOG.info("Started command: {}", command);
      List<Integer> ports = new ArrayList<>();
      int maxAttempts = 100;
      while (--maxAttempts > 0 && ports.size() < 3) {
        try (FileReader fr = new FileReader(new File(portsFilePath));
             BufferedReader br = new BufferedReader(fr)) {
          ports.clear();
          String nextLine;
          while ((nextLine = br.readLine()) != null) {
            ports.add(Integer.parseInt(nextLine));
          }
        } catch (Exception ignore) {
          LOG.debug("Ignoring an exception getting the ports for Tika Process " + runUuid);
        }
        if (ports.size() < 3) {
          try {
            Thread.sleep(50L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      if (ports.size() < 3) {
        throw new RuntimeException("Could not get the ports from Tika process " + runUuid);
      }
      contentInPort = ports.get(0);
      metadataOutPort = ports.get(1);
      contentOutPort = ports.get(2);
    } catch (IOException e) {
      throw new RuntimeException("Could not start tika external with command " + command, e);
    }
  }

  public void close() {
    process.destroy();
    LOG.info("Destroyed TikaProcess that had command: {}", command);
    File propertiesFile = new File(propertiesFilePath);
    if (propertiesFile.exists()) {
      try {
        propertiesFile.delete();
      } catch (Exception e) {
        LOG.debug("Ignoring the exception when file " + propertiesFilePath + " was deleted.");
      }
    }
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        boolean extractHtmlLinks,
                        InputStream contentInStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs) throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    Properties parseProperties = new Properties();
    parseProperties.setProperty("baseUri", baseUri);
    parseProperties.setProperty("contentType", contentType);
    parseProperties.setProperty("extractHtmlLinks", String.valueOf(extractHtmlLinks));
    try (FileOutputStream fis = new FileOutputStream(propertiesFilePath)) {
      parseProperties.store(fis, null);
    } catch (IOException e) {
      throw new RuntimeException("Could not write to properties file: " + propertiesFilePath, e);
    }
    es.execute(() -> {
      try {
        writeContent(contentInPort, contentInStream);
      } catch (Exception e) {
        throw new RuntimeException("Failed to send content stream to forked Tika parser JVM", e);
      }
    });
    Future<Metadata> metadataFuture = es.submit(() -> {
      try {
        return getMetadata(metadataOutPort);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read metadata from forked Tika parser JVM", e);
      }
    });
    Future contentFuture = es.submit(() -> {
      try {
        getContent(contentOutPort, contentOutputStream);
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Failed to read content from forked Tika parser JVM", e);
      }
    });

    Instant mustFinishByInstant = Instant.now().plus(Duration.ofMillis(abortAfterMs));
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

  private void writeContent(int port, InputStream contentInStream) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (OutputStream out = socket.getOutputStream()) {
      long numChars;
      do {
        numChars = IOUtils.copy(contentInStream, out);
      } while (numChars > 0);
    } finally {
      socket.close();
    }
  }

  private Metadata getMetadata(int port) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (InputStream metadataIn = socket.getInputStream()) {
      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
      return (Metadata) objectInputStream.readObject();
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
