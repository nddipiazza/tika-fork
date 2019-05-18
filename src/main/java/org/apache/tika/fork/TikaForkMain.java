package org.apache.tika.fork;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class TikaForkMain {

  public static final int PORT = 9876;

  public static void main(String[] args) throws Exception {
    String host = InetAddress.getLocalHost().getHostAddress();

    File file = new File(args[0]);
    // Get the size of the file
    byte[] bytes = new byte[16 * 1024];
    Socket socket = new Socket(host, PORT);

    // First write the bytes to the tika parser
    try (InputStream in = new FileInputStream(file);
         OutputStream out = socket.getOutputStream();
         //InputStream metadataIn = socket.getInputStream();
    ) {
      int count;
      while ((count = in.read(bytes)) > 0) {
        out.write(bytes,0, count);
      }
      System.out.println("Done sending the bytes!");

      // I want to be able to now read the resulting stream coming from the tika
      // parse response. But when I enable this, it causes the tika input stream to
      // never start.

//      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
//      Metadata metadata = (Metadata)objectInputStream.readObject();
//      System.out.println(metadata);

    } finally {
      socket.close();
    }
  }

  private static Socket getSocket(String host) throws InterruptedException {
    Socket socket;
    int maxRetries = 20;

    while (true) {
      try {
        socket = new Socket(host, PORT);
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
