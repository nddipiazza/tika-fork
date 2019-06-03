package org.apache.tika.fork.main;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.SecureContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

public class ConfigurableAutoDetectParser extends CompositeParser {

  private static final Logger log = LoggerFactory.getLogger(ConfigurableAutoDetectParser.class);

  /**
   * Serial version UID
   */
  private static final long serialVersionUID = 6110455808615143122L;

  /**
   * The type detector used by this parser to auto-detect the type
   * of a document.
   */
  private Detector detector; // always set in the constructor

  /**
   * Maximum compression ratio.
   */
  private long maximumCompressionRatio = 200;

  /**
   * Maximum XML element nesting level.
   */
  private long maximumDepth = 200;

  /**
   * Maximum package entry nesting level.
   */
  private long maximumPackageEntryDepth = 20;
  
  /**
   * Creates an auto-detecting parser instance using the default Tika
   * configuration.
   */
  public ConfigurableAutoDetectParser() {
    this(TikaConfig.getDefaultConfig());
  }

  public ConfigurableAutoDetectParser(Detector detector) {
    this(TikaConfig.getDefaultConfig());
    setDetector(detector);
  }

  /**
   * Creates an auto-detecting parser instance using the specified set of parser.
   * This allows one to create a Tika configuration where only a subset of the
   * available parsers have their 3rd party jars included, as otherwise the
   * use of the default TikaConfig will throw various "ClassNotFound" exceptions.
   *
   * @param parsers
   */
  public ConfigurableAutoDetectParser(Parser... parsers) {
    this(new DefaultDetector(), parsers);
  }

  public ConfigurableAutoDetectParser(Detector detector, Parser... parsers) {
    super(MediaTypeRegistry.getDefaultRegistry(), parsers);
    setDetector(detector);
  }

  public ConfigurableAutoDetectParser(TikaConfig config) {
    super(config.getMediaTypeRegistry(), config.getParser());
    setDetector(config.getDetector());
  }

  public ConfigurableAutoDetectParser(Detector detector, long maxCompressionRatio, long maxDepth, long
      maxPackageEntryDepth) {
    this(detector);
    this.maximumCompressionRatio = maxCompressionRatio;
    this.maximumDepth = maxDepth;
    this.maximumPackageEntryDepth = maxPackageEntryDepth;
  }

  /**
   * Returns the type detector used by this parser to auto-detect the type
   * of a document.
   *
   * @return type detector
   * @since Apache Tika 0.4
   */
  public Detector getDetector() {
    return detector;
  }

  /**
   * Sets the type detector used by this parser to auto-detect the type
   * of a document.
   *
   * @param detector type detector
   * @since Apache Tika 0.4
   */
  public void setDetector(Detector detector) {
    this.detector = detector;
  }

  public void setMaximumCompressionRatio(long maximumCompressionRatio) {
    this.maximumCompressionRatio = maximumCompressionRatio;
  }

  public void setMaximumDepth(int maximumDepth) {
    this.maximumDepth = maximumDepth;
  }

  public void setMaximumPackageEntryDepth(int maximumPackageEntryDepth) {
    this.maximumPackageEntryDepth = maximumPackageEntryDepth;
  }

  public void parse(
      InputStream stream, ContentHandler handler,
      Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    TemporaryResources tmp = new TemporaryResources();
    try {
      TikaInputStream tis = TikaInputStream.get(stream, tmp);

      // Automatically detect the MIME type of the document
      MediaType type = detector.detect(tis, metadata);
      metadata.set(Metadata.CONTENT_TYPE, type.toString());

      // TIKA-216: Zip bomb prevention
      SecureContentHandler sch =
          handler != null ? new SecureContentHandler(handler, tis) : null;
      if (sch != null) {
        log.debug("Setting compression ratio='{}', maxDepth='{}', maxPackageEntryDepth='{}'",
            maximumCompressionRatio, maximumDepth, maximumPackageEntryDepth);
        // NOTE: only modification compared to AutoDetectParser
        sch.setMaximumCompressionRatio(maximumCompressionRatio);
        sch.setMaximumDepth(Math.toIntExact(maximumDepth));
        sch.setMaximumPackageEntryDepth(Math.toIntExact(maximumPackageEntryDepth));
      }
      try {
        // Parse the document
        super.parse(tis, sch, metadata, context);
      } catch (SAXException e) {
        // Convert zip bomb exceptions to TikaExceptions
        sch.throwIfCauseOf(e);
        throw e;
      }
    } finally {
      tmp.dispose();
    }
  }

  public void parse(
      InputStream stream, ContentHandler handler, Metadata metadata)
      throws IOException, SAXException, TikaException {
    ParseContext context = new ParseContext();
    context.set(Parser.class, this);
    parse(stream, handler, metadata, context);
  }

}
