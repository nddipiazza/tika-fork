package org.apache.tika.main;

import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;

public class TikaBodyContentHandler extends ContentHandlerDecorator {

  public static class WriteOutContentHandler extends DefaultHandler {
  
    /**
     * The character stream.
     */
    private final Writer writer;
  
    /**
     * The maximum number of characters to write to the character stream.
     * Set to -1 for no limit.
     */
    private final int writeLimit;
  
    /**
     * Number of characters written so far.
     */
    private int writeCount = 0;
  
    private WriteOutContentHandler(Writer writer, int writeLimit) {
        this.writer = writer;
        this.writeLimit = writeLimit;
    }
  
    /**
     * Creates a content handler that writes character events to
     * the given writer.
     *
     * @param writer writer
     */
    public WriteOutContentHandler(Writer writer) {
        this(writer, -1);
    }
  
    /**
     * Creates a content handler that writes character events to
     * the given output stream using the default encoding.
     *
     * @param stream output stream
     */
    public WriteOutContentHandler(OutputStream stream, Charset charset) {
        this(new OutputStreamWriter(stream, charset));
    }
  
    /**
     * Creates a content handler that writes character events
     * to an internal string buffer. Use the {@link #toString()}
     * method to access the collected character content.
     * <p>
     * The internal string buffer is bounded at the given number of characters.
     * If this write limit is reached, then a {@link SAXException} is thrown.
     * The {@link #isWriteLimitReached(Throwable)} method can be used to
     * detect this case.
     *
     * @since Apache Tika 0.7
     * @param writeLimit maximum number of characters to include in the string,
     *                   or -1 to disable the write limit
     */
    public WriteOutContentHandler(int writeLimit) {
        this(new StringWriter(), writeLimit);
    }
  
    /**
     * Creates a content handler that writes character events
     * to an internal string buffer. Use the {@link #toString()}
     * method to access the collected character content.
     * <p>
     * The internal string buffer is bounded at 100k characters. If this
     * write limit is reached, then a {@link SAXException} is thrown. The
     * {@link #isWriteLimitReached(Throwable)} method can be used to detect
     * this case.
     */
    public WriteOutContentHandler() {
        this(100 * 1000);
    }
  
    /**
     * Writes the given characters to the given character stream.
     */
    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        try {
            if (writeLimit == -1 || writeCount + length <= writeLimit) {
                writer.write(ch, start, length);
                writeCount += length;
            } else {
                writer.write(ch, start, writeLimit - writeCount);
                writeCount = writeLimit;
                throw new WriteLimitReachedException();
            }
        } catch (IOException e) {
            throw new SAXException("Error writing out character content", e);
        }
    }
  
  
    /**
     * Writes the given ignorable characters to the given character stream.
     */
    @Override
    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        characters(ch, start, length);
    }
  
    /**
     * Flushes the character stream so that no characters are forgotten
     * in internal buffers.
     *
     * @see <a href="https://issues.apache.org/jira/browse/TIKA-179">TIKA-179</a>
     * @throws SAXException if the stream can not be flushed
     */
    @Override
    public void endDocument() throws SAXException {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new SAXException("Error flushing character output", e);
        }
    }

    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) throws SAXException {
      // this is to prevent SPAN/DIV text from running together
      try {
        if ("SPAN".equalsIgnoreCase(localName)) {
          if (writeCount > 0) {
            writer.write(" ");
          }
        } else if ("DIV".equalsIgnoreCase(localName)) {
          if (writeCount > 0) {
            writer.write("\n");
          }
        }
      } catch (IOException e) {
        throw new SAXException("Error writing out character content", e);
      }
      super.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) throws SAXException {
      // these break zero-length streams, since we end up always appending a newline.
      try {
        if ("SPAN".equalsIgnoreCase(localName)) {
          writer.write(" ");
        } else if ("DIV".equalsIgnoreCase(localName)) {
          writer.write("\n");
        }
      } catch (IOException e) {
        throw new SAXException("Error writing out character content", e);
      }
      super.endElement(uri, localName, qName);
    }

    /**
     * Returns the contents of the internal string buffer where
     * all the received characters have been collected. Only works
     * when this object was constructed using the empty default
     * constructor or by passing a {@link StringWriter} to the
     * other constructor.
     */
    @Override
    public String toString() {
        return writer.toString();
    }
  
    /**
     * Checks whether the given exception (or any of it's root causes) was
     * thrown by this handler as a signal of reaching the write limit.
     *
     * @since Apache Tika 0.7
     * @param t throwable
     * @return <code>true</code> if the write limit was reached,
     *         <code>false</code> otherwise
     */
    public boolean isWriteLimitReached(Throwable t) {
        if (t instanceof WriteLimitReachedException) {
            return this == ((WriteLimitReachedException) t).getSource();
        } else {
            return t.getCause() != null && isWriteLimitReached(t.getCause());
        }
    }
  
    /**
     * The exception used as a signal when the write limit has been reached.
     */
    private class WriteLimitReachedException extends SAXException {
  
        public WriteLimitReachedException() {
        super("write limit");
      }
  
        public WriteOutContentHandler getSource() {
            return WriteOutContentHandler.this;
        }
  
    }
  
  }

  /**
   * XHTML XPath parser.
   */
  private static final XPathParser PARSER =
    new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  /**
   * The XPath matcher used to select the XHTML body contents.
   */
  private static final Matcher MATCHER =
    PARSER.parse("/xhtml:html/xhtml:body/descendant::node()");

  /**
   * Creates a content handler that passes all XHTML body events to the
   * given underlying content handler.
   *
   * @param handler content handler
   */
  public TikaBodyContentHandler(ContentHandler handler) {
    super(new MatchingContentHandler(handler, MATCHER));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given writer.
   *
   * @param writer writer
   */
  public TikaBodyContentHandler(Writer writer) {
    this(new WriteOutContentHandler(writer));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given output stream using the specified encoding.
   *
   * @param stream output stream
   */
  public TikaBodyContentHandler(OutputStream stream, String encoding) {
    this(new WriteOutContentHandler(stream, Charset.forName(encoding)));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given output stream using the specified Charset.
   *
   * @param stream output stream
   */
  public TikaBodyContentHandler(OutputStream stream, Charset charset) {
    this(new WriteOutContentHandler(stream, charset));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * an internal string buffer. The contents of the buffer can be retrieved
   * using the {@link #toString()} method.
   * <p>
   * The internal string buffer is bounded at the given number of characters.
   * If this write limit is reached, then a {@link SAXException} is thrown.
   *
   * @since Apache Tika 0.7
   * @param writeLimit maximum number of characters to include in the string,
   *                   or -1 to disable the write limit
   */
  public TikaBodyContentHandler(int writeLimit) {
    this(new WriteOutContentHandler(writeLimit));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * an internal string buffer. The contents of the buffer can be retrieved
   * using the {@link #toString()} method.
   * <p>
   * The internal string buffer is bounded at 100k characters. If this write
   * limit is reached, then a {@link SAXException} is thrown.
   */
  public TikaBodyContentHandler() {
    this(new WriteOutContentHandler());
  }

}