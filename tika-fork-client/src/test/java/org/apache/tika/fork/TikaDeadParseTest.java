package org.apache.tika.fork;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tika.client.TikaProcessPool;
import org.apache.tika.metadata.Metadata;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test parses from a web server that freezes up during the request.
 */
public class TikaDeadParseTest {
  private static final Logger LOG = LoggerFactory.getLogger(TikaDeadParseTest.class);

  String tikaDistPath;
  String javaPath = "java";
  Properties parseProperties;
  long maxBytesToParse = 256000000;
  int numThreads = 5;
  int numRepeats = 2;
  int maxRandomDelay = 4000;

  @Before
  public void init() {
    tikaDistPath = ".." + File.separator + "tika-fork-main" + File.separator + "build" + File.separator + "dist";
    parseProperties = new Properties();
    parseProperties.setProperty("parseContent", "true");
  }

  public static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Test
  public void testFrozenParse() throws Exception {
    Integer port = findRandomOpenPortOnAllLocalInterfaces();
    final Server server = new Server(port);
    Thread neverReturnsFromThread = new Thread(() -> {
      try {
        server.setStopTimeout(-1);
        server.setHandler(new AbstractHandler() {
          @Override
          public void handle(String target,
                             Request baseRequest,
                             HttpServletRequest request,
                             HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            LOG.info(target + " is the request");
            response.getWriter().println("start the fun.");
            response.getWriter().flush();
            int count = 10;
            while (--count > 0) {
              try {
                Thread.sleep(1000);
                response.getWriter().println("continue the fun.");
                response.getWriter().flush();
              } catch (InterruptedException e) {
              }
            }
            response.getWriter().println("end the fun.");
            LOG.info("Done with web request.");
          }
        });
        server.start();
      } catch (Exception e) {
        System.err.println("Couldn't create embedded jetty server");
        e.printStackTrace();
      }
    });
    try (TikaProcessPool tikaProcessPool = new TikaProcessPool(javaPath,
        System.getProperty("java.io.tmpdir"),
        tikaDistPath,
        200,
        parseProperties,
        -1,
        -1,
        20,
        true,
        6000,
        3000,
        -1,
        -1)) {
      AtomicReference<Exception> exc = new AtomicReference<>();
      neverReturnsFromThread.start();
      Runnable r = () -> {
        for (int i = 0; i < numRepeats; ++i) {
          try {
            LOG.info("Thread {} repeat {}", Thread.currentThread().getId(), i);
            Thread.sleep(new Random().nextInt(maxRandomDelay));
            String uri = "http://" + InetAddress.getLocalHost().getHostAddress() + ":" + port;
            LOG.info("Server is started on {}", uri);
            CloseableHttpClient client = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(uri);
            ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
            try (InputStream is = client.execute(httpGet).getEntity().getContent()) {
              LOG.info("Starting the parse");
              Metadata metadata = tikaProcessPool.parse(uri,
                  "text/html",
                  is,
                  contentOutputStream,
                  3000L,
                  maxBytesToParse
              );
            } catch (TimeoutException timeoutEx) {
              LOG.info("Timeout exception. Good.", timeoutEx);
            }
          } catch (IOException e) {
            LOG.info("Exception is OK - couldn't close the socket.");
          } catch (Exception e) {
            exc.set(e);
            return;
          }
        }
      };
      List<Thread> ts = new ArrayList<>();
      for (int i = 0; i < numThreads; ++i) {
        Thread t = new Thread(r);
        t.start();
        ts.add(t);
      }
      for (Thread t : ts) {
        t.join();
      }
      if (exc.get() != null) {
        throw exc.get();
      }
    }
  }
}