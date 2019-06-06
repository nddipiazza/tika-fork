package org.apache.tika.fork.main;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.NullOutputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TikaForkMain {
  private static final Logger LOG = LoggerFactory.getLogger(TikaForkMain.class);

  private static TikaParsingHandler getContentHandler(String mainUrl,
                                                      Metadata metadata,
                                                      OutputStream out,
                                                      boolean extractHtmlLinks,
                                                      int maxBytes) throws TikaException {
    ContentHandler main = new TikaBodyContentHandler(maxBytes);

    TikaLinkContentHandler linksHandler = null;
    if (extractHtmlLinks) {
      linksHandler = new TikaLinkContentHandler(mainUrl, true);
    }
    return new TikaParsingHandler(mainUrl, out, main, linksHandler);
  }

  @Option(name = "-configDirectoryPath", usage = "The directory that will contain the configuration files that communicate between the fork process and the client process.")
  private String configDirectoryPath;
  @Option(name = "-parserPropertiesFilePath", usage = "The parse configuration file.")
  private String parserPropertiesFilePath;
  @Option(name = "-contentInServerPort", usage = "This is the port for the socket server that will be used to send in the file.")
  private int contentInServerPort = 0;
  @Option(name = "-metadataOutServerPort", usage = "This is the port for the socket server that will be used to send out the parsed metadata.")
  private int metadataOutServerPort = 0;
  @Option(name = "-contentOutServerPort", usage = "This is the port for the socket server that will be used to send out the parsed file contents.")
  private int contentOutServerPort = 0;

  private ServerSocket contentInServerSocket;
  private ServerSocket metadataOutServerSocket;
  private ServerSocket contentOutServerSocket;

  private Properties parserProperties;
  private ConfigurableAutoDetectParser defaultParser;
  private Detector detector = new DefaultDetector();

  boolean extractHtmlLinks;
  boolean includeImages;

  private void run() throws Exception {
    parserProperties = new Properties();
    if (StringUtils.isNotBlank(parserPropertiesFilePath)) {
      try (FileReader fr = new FileReader(parserPropertiesFilePath)) {
        parserProperties.load(fr);
      }
    }
    defaultParser = new ConfigurableAutoDetectParser(detector,
      Integer.parseInt(parserProperties.getProperty("zipBombCompressionRatio", "200")),
      Integer.parseInt(parserProperties.getProperty("zipBombMaxDepth", "200")),
      Integer.parseInt(parserProperties.getProperty("zipBombMaxPackageEntryDepth", "20")));
    if (StringUtils.isBlank(configDirectoryPath)) {
      configDirectoryPath = System.getProperty("java.io.tmpdir");
    } else {
      if (configDirectoryPath.endsWith(File.separator)) {
        configDirectoryPath = configDirectoryPath.substring(0, configDirectoryPath.length() - 1);
      }
    }
    extractHtmlLinks = Boolean.parseBoolean(parserProperties.getProperty("extractHtmlLinks", "false"));
    includeImages = Boolean.parseBoolean(parserProperties.getProperty("includeImages", "false"));
    ExecutorService keepAliveEs = Executors.newSingleThreadExecutor();
    boolean parseContent = Boolean.parseBoolean(parserProperties.getProperty("parseContent", "true"));
    String portsFilePath = configDirectoryPath + File.separator + "tikafork-ports-" + parserProperties.get("runUuid") + ".properties";
    LOG.info("Tika ports file path: \"{}\"", portsFilePath);
    File portsFile = new File(portsFilePath);
    if (parseContent) {
      ExecutorService es = Executors.newFixedThreadPool(3);
      try {
        contentInServerSocket = new ServerSocket(contentInServerPort);
        metadataOutServerSocket = new ServerSocket(metadataOutServerPort);
        contentOutServerSocket = new ServerSocket(contentOutServerPort);

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
    } else {
      ExecutorService es = Executors.newFixedThreadPool(2);

      try {
        contentInServerSocket = new ServerSocket(contentInServerPort);
        metadataOutServerSocket = new ServerSocket(metadataOutServerPort);

        FileUtils.writeLines(portsFile,
          Lists.newArrayList(
            String.valueOf(contentInServerSocket.getLocalPort()),
            String.valueOf(metadataOutServerSocket.getLocalPort())
          )
        );

        while (true) {
          final PipedInputStream metadataInputStream = new PipedInputStream();
          final PipedOutputStream metadataOutputStream = new PipedOutputStream();

          metadataInputStream.connect(metadataOutputStream);

          final OutputStream contentOutputStream = new NullOutputStream();

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

          metadataFuture.get();
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
          if (metadataOutServerSocket != null && metadataOutServerSocket.isBound()) {
            metadataOutServerSocket.close();
          }
        } catch (IOException e) {
          LOG.debug("Could not close metadata out socket server", e);
        }
        FileUtils.deleteQuietly(portsFile);
      }
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

    // collect extended set of elements
    context.set(HtmlMapper.class, ExtendedHtmlMapper.INSTANCE);
    context.set(Parser.class, defaultParser);

    TikaConfig config = TikaConfig.getDefaultConfig();
    Metadata metadata = new Metadata();
    CompositeParser compositeParser = new CompositeParser(config.getMediaTypeRegistry(), config.getParser());

    try (Socket socket = contentInServerSocket.accept();
         InputStream inputStream = socket.getInputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(metadataOutputStream)) {

      if (includeImages) {
        PDFParserConfig pdfParserConfig = new PDFParserConfig();
        pdfParserConfig.setExtractInlineImages(true);
        context.set(PDFParserConfig.class, pdfParserConfig);
      }

      String baseUri = "";
      String contentType = "";
      int nextChar;
      while ((nextChar = inputStream.read()) != '\n') {
        baseUri += (char)nextChar;
      }
      while ((nextChar = inputStream.read()) != '\n') {
        contentType += (char)nextChar;
      }

      LOG.info("Next file to parse baseUri={}, contentType={}", baseUri, contentType);

      if (StringUtils.isNotBlank(contentType)) {
        metadata.set(Metadata.CONTENT_TYPE, contentType);
      }

      TikaInputStream tikaInputStream = TikaInputStream.get(inputStream);

      TikaParsingHandler contentHandler = getContentHandler(baseUri, metadata, contentOutputStream, extractHtmlLinks, 200);
      compositeParser.parse(tikaInputStream, contentHandler, metadata, context);

      objectOutputStream.writeObject(metadata);
    } finally {
      contentOutputStream.close();
    }
  }

  /**
   * Runs the external tika parsing server.
   */
  public static void main(String[] args) throws Exception {
    TikaForkMain tikaForkMain = new TikaForkMain();
    CmdLineParser cmdLineParser = new CmdLineParser(tikaForkMain);
    cmdLineParser.parseArgument(args);
    tikaForkMain.run();
  }
}
