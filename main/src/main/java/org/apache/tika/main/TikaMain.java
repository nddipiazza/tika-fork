package org.apache.tika.main;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
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

  private static final int maxStackDepth = 20;
  private static final int maxBufferSize = 1024 * 1024 * 10;
  private static final int defaultBufferSize = 1024;

  private static int getBufferSize(String sizeString) {
    int bufferSize = defaultBufferSize;
    if (sizeString != null && !sizeString.isEmpty()) {
      try {
        bufferSize = Integer.parseInt(sizeString);
      } catch (Exception e) {
        // ignore
      }
      if (bufferSize < 0) {
        bufferSize = defaultBufferSize;
      } else if (bufferSize > maxBufferSize) {
        bufferSize = maxBufferSize;
      }
    }
    return bufferSize;
  }

  private static TikaParsingHandler getContentHandler(String mainUrl,
                                                      Metadata metadata,
                                                      OutputStream out,
                                                      boolean extractHtmlLinks) throws TikaException {
    String sizeString = metadata.get(Metadata.CONTENT_LENGTH);
    final int bufferSize = getBufferSize(sizeString);
    String contentType = metadata.get(Metadata.CONTENT_TYPE);
    if (contentType == null) {
      contentType = "application/octet-stream";
    }

    ContentHandler main = new TikaBodyContentHandler(out, TikaConstants.defaultOutputEncoding);

    TikaLinkContentHandler linksHandler = null;
    if (extractHtmlLinks) {
      linksHandler = new TikaLinkContentHandler(mainUrl, true);
    }
    return new TikaParsingHandler(mainUrl, out, main, linksHandler);
  }

  private int contentInPort;
  private int metadataOutPort;
  private int contentOutPort;

  public TikaMain(int contentInPort, int metadataOutPort, int contentOutPort) {
    this.contentInPort = contentInPort;
    this.metadataOutPort = metadataOutPort;
    this.contentOutPort = contentOutPort;
  }

  private void run() throws Exception {
    ExecutorService es = Executors.newFixedThreadPool(3);

    while (true) {
      LOG.info("Waiting for the next input on contentInPort={}, contentOutPort={}, metadataOutPort={}", contentInPort, contentOutPort, metadataOutPort);

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
          throw new RuntimeException("Could not parse file", e);
        }
      });

      Future metadataFuture = es.submit(() -> {
        try {
          writeMetadata(metadataInputStream);
        } catch (Exception e) {
          throw new RuntimeException("Could not write metadata", e);
        }
      });

      Future contentFuture = es.submit(() -> {
        try {
          writeContent(contentInputStream);
        } catch (Exception e) {
          throw new RuntimeException("Could not write content", e);
        }
      });

      metadataFuture.get();
      contentFuture.get();
    }
  }

  private void writeMetadata(InputStream inputStream) throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(metadataOutPort);
         Socket socket = serverSocket.accept();
         OutputStream outputStream = socket.getOutputStream()
    ) {
      long numRead;
      do {
        numRead = IOUtils.copy(inputStream, outputStream);
      } while (numRead > 0);
    }
  }

  private void writeContent(InputStream inputStream) throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(contentOutPort);
         Socket socket = serverSocket.accept();
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
    Detector detector = new DefaultDetector();
    TikaConfig config = TikaConfig.getDefaultConfig();
    Metadata metadata = new Metadata();
    CompositeParser compositeParser = new CompositeParser(config.getMediaTypeRegistry(), config.getParser());
    try (ServerSocket serverSocket = new ServerSocket(contentInPort);
         Socket socket = serverSocket.accept();
         InputStream inputStream = socket.getInputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(metadataOutputStream)) {

      Properties parseProperties = new Properties();
      String propertiesFilePath = System.getProperty("java.io.tmpdir") + File.separator + "tika-fork-" + contentInPort + ".properties";
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
      TikaInputStream tikaInputStream = TikaInputStream.get(inputStream);

      TikaParsingHandler contentHandler = getContentHandler(baseUri, metadata, contentOutputStream, extractHtmlLinks);
      compositeParser.parse(tikaInputStream, contentHandler, metadata, context);

      objectOutputStream.writeObject(metadata);
    }
    contentOutputStream.close();
  }

  public static void main(String[] args) throws Exception {
    TikaMain tikaMain = new TikaMain(Integer.parseInt(args[0]),
      Integer.parseInt(args[1]),
      Integer.parseInt(args[2]));
    tikaMain.run();
  }
}
