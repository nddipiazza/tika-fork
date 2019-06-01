package org.apache.tika.main;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * Useful for kubernetes. Gives you something for the keepalive check.
 * Will return the number of milliseconds since it has seen a parse request.
 */
public class HealthCheckServer implements Runnable {
  public static long LAST_UPDATE = System.currentTimeMillis();

  private static final String newLine = "\r\n";

  int port;

  public HealthCheckServer(int port) {
    this.port = port;
  }

  @Override
  public void run() {
    try {
      ServerSocket socket = new ServerSocket(port);

      while (true) {
        Socket connection = socket.accept();

        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
          OutputStream out = new BufferedOutputStream(connection.getOutputStream());
          PrintStream pout = new PrintStream(out);

          // read first line of request
          String request = in.readLine();
          if (request == null) {
            continue;
          }

          // we ignore the rest
          while (true) {
            String ignore = in.readLine();
            if (ignore == null || ignore.length() == 0) {
              break;
            }
          }

          if (!request.startsWith("GET ") ||
            !(request.endsWith(" HTTP/1.0") || request.endsWith(" HTTP/1.1"))) {
            // bad request
            pout.print("HTTP/1.0 400 Bad Request" + newLine + newLine);
          } else {
            String response = String.valueOf(System.currentTimeMillis() - LAST_UPDATE);

            pout.print(
              "HTTP/1.0 200 OK" + newLine +
                "Content-Type: text/plain" + newLine +
                "Date: " + new Date() + newLine +
                "Content-length: " + response.length() + newLine + newLine +
                response
            );
          }

          pout.close();
        } catch (Throwable tri) {
          System.err.println("Error handling request: " + tri);
        }
      }
    } catch (Throwable tr) {
      System.err.println("Could not start server: " + tr);
    }

  }
}