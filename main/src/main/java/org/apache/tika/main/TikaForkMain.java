package org.apache.tika.main;

import org.apache.commons.io.IOUtils;
import org.apache.tika.metadata.Metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TikaForkMain {

  public static final int PORT = 9876;

  private File file;

  public static void main(String[] args) throws Exception {
    TikaForkMain tikaForkMain = new TikaForkMain(args[0]);
    tikaForkMain.parseFile();
  }

  public TikaForkMain(String filePath) throws Exception {
    this.file = new File(filePath);
  }

  private void parseFile() throws InterruptedException, java.util.concurrent.ExecutionException {
    ExecutorService es = Executors.newFixedThreadPool(3);
    es.execute(() -> {
      try {
        writeFile(PORT, file);
      } catch (Exception e) {
        throw new RuntimeException("Could not write file", e);
      }
    });
    Future<Metadata> metadataFuture = es.submit(() -> {
      try {
        return getMetadata(PORT + 1);
      } catch (Exception e) {
        throw new RuntimeException("Could not write file", e);
      }
    });
    Future<byte[]> contentFuture = es.submit(() -> {
      try {
        return getContent(PORT + 2);
      } catch (Exception e) {
        throw new RuntimeException("Could not write file", e);
      }
    });

    Metadata metadata = metadataFuture.get();
    System.out.println("Read the metadata!");
    System.out.println(metadata);

    byte[] content = contentFuture.get();
    System.out.println("Read the content!");
    System.out.println(new String(content));

    es.shutdown();
  }

  private void writeFile(int port, File file) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    // First write the bytes to the tika parser
    try (InputStream in = new FileInputStream(file);
         OutputStream out = socket.getOutputStream()
    ) {
      long numChars;
      do {
        numChars = IOUtils.copy(in, out);
      } while (numChars > 0);
      System.out.println("Done sending the bytes!");
    } finally {
      socket.close();
    }
  }

  private Metadata getMetadata(int port) throws Exception {
    Socket socket = getSocket(InetAddress.getLocalHost().getHostAddress(), port);
    try (InputStream metadataIn = socket.getInputStream()) {
      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
      return (Metadata)objectInputStream.readObject();
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
