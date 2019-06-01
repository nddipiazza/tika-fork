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
import java.util.Scanner;
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

  static private final long WAIT_FOR_PORTS_FOR_MAX_MS = Long.parseLong(System.getProperty("org.apache.tika.ports.timeout.ms", String.valueOf(2 * 60 * 60 * 1000)));

  private int contentInPort;
  private int metadataOutPort;
  private int contentOutPort;
  private Process process;
  private List<String> command;
  private String runUuid = UUID.randomUUID().toString();
  private String parseConfigPropertiesFilePath;
  private String parseContextPropertiesFilePath;
  private String portsFilePath;
  private boolean parseContent;

  public TikaProcess(String javaPath,
                     String configDirectoryPath,
                     String tikaDistPath,
                     int tikaMaxHeapSizeMb,
                     Properties parserProperties) {
    parseContent = Boolean.parseBoolean(parserProperties.getProperty("parseContent", "false"));

    parseConfigPropertiesFilePath = configDirectoryPath + File.separator + "tikafork-config-" + runUuid + ".properties";
    parseContextPropertiesFilePath = configDirectoryPath + File.separator + "tikafork-context-" + runUuid + ".properties";
    portsFilePath = configDirectoryPath + File.separator + "tikafork-ports-" + runUuid + ".properties";

    command = new ArrayList<>();
    command.add(javaPath == null || javaPath.trim().length() == 0 ? CURRENT_JAVA_BINARY : javaPath);
    if (tikaMaxHeapSizeMb > 0) {
      command.add("-Xmx" + tikaMaxHeapSizeMb + "m");
    }
    command.add("-cp");
    command.add(tikaDistPath + File.separator + "*");
    command.add("org.apache.tika.main.TikaForkMain");

    Properties sendParseProperties = (Properties)parserProperties.clone();
    sendParseProperties.setProperty("runUuid", runUuid);
    try (FileOutputStream fos = new FileOutputStream(parseConfigPropertiesFilePath)) {
      sendParseProperties.store(fos, null);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't save the parse properties file", e);
    }

    command.add("-parserPropertiesFilePath");
    command.add(parseConfigPropertiesFilePath);
    if (configDirectoryPath != null && configDirectoryPath.trim().length() > 0) {
      command.add("-configDirectoryPath");
      command.add(configDirectoryPath);
    }
    try {
      process = new ProcessBuilder(command)
        .start();
      inheritIO(process.getInputStream());
      inheritIO(process.getErrorStream());
      LOG.info("Started command: {}", command);
      List<Integer> ports = new ArrayList<>();

      // Wait for the sockets file for up to WAIT_FOR_PORTS_FOR_MAX_MS
      int numPortsToWaitFor = parseContent ? 3 : 2;
      Instant stopWaitingOn = Instant.now().plus(Duration.ofMillis(WAIT_FOR_PORTS_FOR_MAX_MS));
      while (Instant.now().isBefore(stopWaitingOn) && ports.size() < numPortsToWaitFor) {
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
        if (ports.size() < numPortsToWaitFor) {
          if (!process.isAlive()) {
            throw new RuntimeException("Process died with exit code " + process.exitValue());
          }
          try {
            Thread.sleep(50L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      if (ports.size() < numPortsToWaitFor) {
        throw new RuntimeException("Could not get the ports from Tika process " + runUuid +
          " after " + WAIT_FOR_PORTS_FOR_MAX_MS + " ms. This timeout is specified by the org.apache.tika.ports.timeout.ms system property.");
      }
      contentInPort = ports.get(0);
      metadataOutPort = ports.get(1);
      if (parseContent) {
        contentOutPort = ports.get(2);
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not start tika external with command " + command, e);
    }
  }

  public void close() {
    process.destroy();
    LOG.info("Destroyed TikaProcess that had command: {}", command);
    File parseContextPropertiesFile = new File(parseContextPropertiesFilePath);
    if (parseContextPropertiesFile.exists()) {
      try {
        parseContextPropertiesFile.delete();
      } catch (Exception e) {
        LOG.debug("Ignoring the exception when file " + parseContextPropertiesFilePath + " was deleted.");
      }
    }
    File parseConfigPropertiesFile = new File(parseConfigPropertiesFilePath);
    if (parseConfigPropertiesFile.exists()) {
      try {
        parseConfigPropertiesFile.delete();
      } catch (Exception e) {
        LOG.debug("Ignoring the exception when file " + parseConfigPropertiesFilePath + " was deleted.");
      }
    }
  }

  public Metadata parse(String baseUri,
                        String contentType,
                        InputStream contentInStream,
                        OutputStream contentOutputStream,
                        long abortAfterMs) throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    es.execute(() -> {
      try {
        writeContent(baseUri, contentType, contentInPort, contentInStream);
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
                            InputStream contentInStream) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (OutputStream out = socket.getOutputStream()) {
      out.write(baseUri.getBytes());
      out.write('\n');
      out.write(contentType.getBytes());
      out.write('\n');
//      out.write("false".getBytes());
//      out.write('\n');
//      out.write("false".getBytes());
//      out.write('\n');
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

  private void inheritIO(final InputStream src) {
    new Thread(() -> {
      Scanner sc = new Scanner(src);
      while (sc.hasNextLine()) {
        String nextLine = sc.nextLine();
        LOG.info(nextLine);
      }
    }).start();
  }
}
