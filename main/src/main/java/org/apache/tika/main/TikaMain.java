package org.apache.tika.main;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TikaMain {
  private static final Logger LOG = LoggerFactory.getLogger(TikaMain.class);

  private static TikaParsingHandler getContentHandler(String mainUrl,
                                                      Metadata metadata,
                                                      OutputStream out,
                                                      boolean extractHtmlLinks) throws TikaException {
    ContentHandler main = new TikaBodyContentHandler(out, TikaConstants.defaultOutputEncoding);

    TikaLinkContentHandler linksHandler = null;
    if (extractHtmlLinks) {
      linksHandler = new TikaLinkContentHandler(mainUrl, true);
    }
    return new TikaParsingHandler(mainUrl, out, main, linksHandler);
  }

  private ServerSocket contentInServerSocket;
  private ServerSocket metadataOutServerSocket;
  private ServerSocket contentOutServerSocket;
  private String runUuid;

  public TikaMain(String runUuid) {
    this.runUuid = runUuid;
  }

  private void run() throws Exception {
    ExecutorService es = Executors.newFixedThreadPool(3);
    String portsFilePath = System.getProperty("java.io.tmpdir") + File.separator + "tika-ports-" + runUuid + ".properties";
    File portsFile = new File(portsFilePath);

    try {
      contentInServerSocket = new ServerSocket(0);
      metadataOutServerSocket = new ServerSocket(0);
      contentOutServerSocket = new ServerSocket(0);

      FileUtils.writeLines(portsFile,
        Lists.newArrayList(
          String.valueOf(contentInServerSocket.getLocalPort()),
          String.valueOf(metadataOutServerSocket.getLocalPort()),
          String.valueOf(contentOutServerSocket.getLocalPort())
        )
      );

      while (true) {
        final PipedInputStream metadataInputStream = new PipedInputStream();
        final PipedOutputStream metadataOutputStream = new PipedOutputStream();

        metadataInputStream.connect(metadataOutputStream);

        final PipedInputStream contentInputStream = new PipedInputStream();
        final PipedOutputStream contentOutputStream = new PipedOutputStream();

        contentInputStream.connect(contentOutputStream);

        es.execute(() -> {
          try {
            parseFile(metadataOutputStream, contentOutputStream);
          } catch (Exception e) {
            try {
              contentOutputStream.close();
            } catch (IOException e1) {
              LOG.debug("Couldn't close content output stream.");
            }
            throw new RuntimeException("Could not parse file", e);
          }
        });

        Future metadataFuture = es.submit(() -> {
          try {
            writeMetadata(metadataInputStream);
          } catch (Exception e) {
            try {
              metadataOutputStream.close();
            } catch (IOException e1) {
              LOG.debug("Couldn't close metadata output stream.");
            }
            throw new RuntimeException("Could not write metadata", e);
          }
        });

        Future contentFuture = es.submit(() -> {
          try {
            writeContent(contentInputStream);
          } catch (Exception e) {
            try {
              contentInputStream.close();
            } catch (IOException e1) {
              LOG.debug("Couldn't close content input stream.");
            }
            throw new RuntimeException("Could not write content", e);
          }
        });

        metadataFuture.get();
        contentFuture.get();
      }
    } finally {
      try {
        if (contentInServerSocket != null && contentInServerSocket.isBound()) {
          contentInServerSocket.close();
        }
      } catch (IOException e) {
        LOG.debug("Could not close content in socket server", e);
      }
      try {
        if (contentOutServerSocket != null && contentOutServerSocket.isBound()) {
          contentOutServerSocket.close();
        }
      } catch (IOException e) {
        LOG.debug("Could not close content out socket server", e);
      }
      try {
        if (metadataOutServerSocket != null && metadataOutServerSocket.isBound()) {
          metadataOutServerSocket.close();
        }
      } catch (IOException e) {
        LOG.debug("Could not close metadata out socket server", e);
      }
      FileUtils.deleteQuietly(portsFile);
    }
  }

  private void writeMetadata(InputStream inputStream) throws Exception {
    try (Socket socket = metadataOutServerSocket.accept();
         OutputStream outputStream = socket.getOutputStream()
    ) {
      long numRead;
      do {
        numRead = IOUtils.copy(inputStream, outputStream);
      } while (numRead > 0);
    }
  }

  private void writeContent(InputStream inputStream) throws Exception {
    try (Socket socket = contentOutServerSocket.accept();
         OutputStream outputStream = socket.getOutputStream()
    ) {
      long numRead;
      do {
        numRead = IOUtils.copy(inputStream, outputStream);
      } while (numRead > 0);
    }
  }

  private void parseFile(OutputStream metadataOutputStream, OutputStream contentOutputStream) throws Exception {
    ParseContext context = new ParseContext();
    TikaConfig config = TikaConfig.getDefaultConfig();
    Metadata metadata = new Metadata();
    CompositeParser compositeParser = new CompositeParser(config.getMediaTypeRegistry(), config.getParser());

    try (Socket socket = contentInServerSocket.accept();
         InputStream inputStream = socket.getInputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(metadataOutputStream)) {


      Properties parseProperties = new Properties();
      String propertiesFilePath = System.getProperty("java.io.tmpdir") + File.separator + "tika-fork-" + runUuid + ".properties";
      if (!Files.exists(Paths.get(propertiesFilePath))) {
        throw new Exception("Cannot find property file: " + propertiesFilePath);
      }
      parseProperties.load(new FileInputStream(propertiesFilePath));

      String baseUri = parseProperties.getProperty("baseUri");
      if (baseUri == null) {
        throw new Exception("Missing property baseUri from the properties file " + propertiesFilePath);
      }

      String contentType = parseProperties.getProperty("contentType");
      if (StringUtils.isNotBlank(contentType)) {
        metadata.set(Metadata.CONTENT_TYPE, contentType);
      }

      boolean extractHtmlLinks = Boolean.parseBoolean(parseProperties.getProperty("extractHtmlLinks", "false"));

      LOG.info("Next file - baseUri={}, contentType={}, extractHtmlLinks={}", baseUri, contentType, extractHtmlLinks);

      TikaInputStream tikaInputStream = TikaInputStream.get(inputStream);

      TikaParsingHandler contentHandler = getContentHandler(baseUri, metadata, contentOutputStream, extractHtmlLinks);
      compositeParser.parse(tikaInputStream, contentHandler, metadata, context);

      objectOutputStream.writeObject(metadata);
    } finally {
      contentOutputStream.close();
    }
  }

  public static void main(String[] args) throws Exception {
    TikaMain tikaMain = new TikaMain(args[0]);
    tikaMain.run();
  }
}
