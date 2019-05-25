package org.apache.tika.fork;

import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TikaProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcess.class);

  private int contentInPort;
  private int metadataOutPort;
  private int contentOutPort;
  private Process process;
  private List<String> command;

  public static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public TikaProcess(String tikaDistPath, int tikaMaxHeapSizeMb) throws IOException {
    this.contentInPort = findRandomOpenPortOnAllLocalInterfaces();
    this.metadataOutPort = findRandomOpenPortOnAllLocalInterfaces();
    this.contentOutPort = findRandomOpenPortOnAllLocalInterfaces();
    command = new ArrayList<>();
    command.add("java");
    if (tikaMaxHeapSizeMb > 0) {
      command.add("-Xmx" + tikaMaxHeapSizeMb + "m");
    }
    command.add("-cp");
    command.add(tikaDistPath + File.separator + "*");
    command.add("org.apache.tika.main.TikaMain");
    command.add(String.valueOf(contentInPort));
    command.add(String.valueOf(metadataOutPort));
    command.add(String.valueOf(contentOutPort));
    try {
      process = new ProcessBuilder(command)
        .inheritIO()
        .start();
      LOG.info("Started command: {}", command);
    } catch (IOException e) {
      throw new RuntimeException("Could not start tika external with command " + command, e);
    }
  }

  public void close() {
    process.destroy();
    LOG.info("Destroyed TikaProcess that had command: {}", command);
  }

  public Metadata parse(String baseUri, String contentType, boolean extractHtmlLinks, InputStream contentInStream, OutputStream contentOutputStream) throws InterruptedException, ExecutionException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    String propertiesFilePath = System.getProperty("java.io.tmpdir") + File.separator + "tika-fork-" + contentInPort + ".properties";
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
        throw new RuntimeException("Could not write file", e);
      }
    });
    Future<Metadata> metadataFuture = es.submit(() -> {
      try {
        return getMetadata(metadataOutPort);
      } catch (Exception e) {
        throw new RuntimeException("Could not write metadata to metadataOutPort", e);
      }
    });
    Future contentFuture = es.submit(() -> {
      try {
        getContent(contentOutPort, contentOutputStream);
      } catch (Exception e) {
        throw new RuntimeException("Could not write content to contentOutPort", e);
      }
    });

    contentFuture.get();
    Metadata metadataResult = metadataFuture.get();

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
