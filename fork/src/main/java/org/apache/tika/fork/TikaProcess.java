package org.apache.tika.fork;

import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TikaProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TikaProcess.class);

  private String tikaDistPath;
  private int dataInPort;
  private int metadataOutPort;
  private int contentOutPort;
  private Process process;

  public static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public TikaProcess(String tikaDistPath) throws IOException {
    this.tikaDistPath = tikaDistPath;
    this.dataInPort = findRandomOpenPortOnAllLocalInterfaces();
    this.metadataOutPort = findRandomOpenPortOnAllLocalInterfaces();
    this.contentOutPort = findRandomOpenPortOnAllLocalInterfaces();
    List<String> command = new ArrayList<>();
    command.add("java");
    command.add("-cp");
    command.add(tikaDistPath + File.separator + "*");
    command.add("org.apache.tika.main.TikaMain");
    command.add(String.valueOf(dataInPort));
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
  }

  public Metadata parse(InputStream contentInStream) throws InterruptedException, ExecutionException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    es.execute(() -> {
      try {
        writeContent(dataInPort, contentInStream);
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
    Future<byte[]> contentFuture = es.submit(() -> {
      try {
        return getContent(contentOutPort);
      } catch (Exception e) {
        throw new RuntimeException("Could not write content to contentOutPort", e);
      }
    });

    byte[] content = contentFuture.get();
    LOG.info("Read the content - {}", new String(content));

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
      LOG.info("Done sending the bytes!");
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

  private byte[] getContent(int port) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (InputStream contentIn = socket.getInputStream()) {
      return IOUtils.toByteArray(contentIn);
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
