package org.apache.tika.fork;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;

public class TikaMain {

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

  private static TikaParsingHandler getContentHandler(String mainUrl, Metadata metadata, boolean extractHtmlLinks) throws TikaException {
    String sizeString = metadata.get(Metadata.CONTENT_LENGTH);
    final int bufferSize = getBufferSize(sizeString);
    String contentType = metadata.get(Metadata.CONTENT_TYPE);
    if (contentType == null) {
      contentType = "application/octet-stream";
    }

    // TODO - i want to send this to the socket output stream
    ByteArrayOutputStream out = new ByteArrayOutputStream(bufferSize);
    ContentHandler main = new TikaBodyContentHandler(out, TikaConstants.defaultOutputEncoding);

    TikaLinkContentHandler linksHandler = null;
    if (extractHtmlLinks) {
      linksHandler = new TikaLinkContentHandler(mainUrl, true);
    }
    return new TikaParsingHandler(mainUrl, out, main, linksHandler);
  }

  public static void main(String[] args) throws Exception {

    System.out.println("args: " + args[0] + " " + args[1]);

    ParseContext context = new ParseContext();
    Detector detector = new DefaultDetector();
    TikaConfig config = TikaConfig.getDefaultConfig();
    Optional<String> contentType = args.length > 2 ? Optional.of(args[2]) : Optional.empty();
    Metadata metadata = new Metadata();
    if (contentType.isPresent()) {
      metadata.set(Metadata.CONTENT_TYPE, contentType.get());
    }
    CompositeParser compositeParser = new CompositeParser(config.getMediaTypeRegistry(), config.getParser());
    try (ServerSocket serverSocket = new ServerSocket(Integer.parseInt(args[0]));
         Socket socket = serverSocket.accept();
         InputStream inputStream = socket.getInputStream();
         //ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
         ) {
      TikaInputStream tikaInputStream = TikaInputStream.get(inputStream);

      // I want to send the output stream of the socket here so that the body of
      // tika write to the socket output stream!
      TikaParsingHandler contentHandler = getContentHandler(args[1], metadata, false);
      compositeParser.parse(tikaInputStream, contentHandler, metadata, context);

      System.out.println("Got metadata! " + metadata);
      System.out.println("Got content! " + contentHandler.getOutput().toString());

      // I can't figure out how to get this to work. The stream freezes when parsing after
      // i add it.

//      objectOutputStream.writeObject(metadata);
//      objectOutputStream.flush();
    }
  }
}
