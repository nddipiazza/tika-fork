package org.apache.tika.fork;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.tika.metadata.Metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class TikaForkMain {

  public static final int PORT = 9876;

  public static void main(String[] args) throws Exception {
//    Process p = new ProcessBuilder(Lists.newArrayList("java",
//      "-cp",
//      "build/libs/tika-fork-all-1.0.jar",
//      "org.apache.tika.fork.TikaMain",
//      String.valueOf(PORT),
//      "http://unec.edu.az/application/uploads/2014/12/pdf-sample.pdf",
//      "application/pdf"))
//      .inheritIO()
//      .start();
    String host = InetAddress.getLocalHost().getHostAddress();
    Socket socket;

//    int maxRetries = 20;
//
//    while (true) {
//      try {
//        if (!p.isAlive()) {
//          throw new RuntimeException("Process terminated abnormally");
//        }
//        socket = new Socket(host, PORT);
//        if (socket != null || --maxRetries < 0) {
//          break;
//        }
//      } catch (IOException e) {
//        Thread.sleep(1000);
//      }
//    }

    File file = new File("/home/ndipiazza/Downloads/pdf-sample.pdf");
    // Get the size of the file
    byte[] bytes = new byte[16 * 1024];
    socket = new Socket(host, PORT);

    // First write the bytes to the tika parser
    try (InputStream in = new FileInputStream(file);
         OutputStream out = socket.getOutputStream();
         InputStream metadataIn = socket.getInputStream();
    ) {
      int count;
      while ((count = in.read(bytes)) > 0) {
        out.write(bytes,0, count);
      }
      System.out.println("Done sending the bytes!");
      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
      Metadata metadata = (Metadata)objectInputStream.readObject();
      System.out.println(metadata);
    } finally {
      socket.close();
    }

    // now get the response
    socket = new Socket(host, PORT);
    try (InputStream metadataIn = socket.getInputStream()) {
      ObjectInputStream objectInputStream = new ObjectInputStream(metadataIn);
      Metadata metadata = (Metadata)objectInputStream.readObject();
      System.out.println(metadata);
    } finally {
      socket.close();
    }
  }
}
