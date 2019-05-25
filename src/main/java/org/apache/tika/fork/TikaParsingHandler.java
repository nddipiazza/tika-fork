package org.apache.tika.fork;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.OutputStream;

/**
 * Like Tika's {@link org.apache.tika.sax.TeeContentHandler} only it allows accessing the handlers.
 */
public class TikaParsingHandler extends DefaultHandler {
  private final ContentHandler mainHandler;
  private final TikaLinkContentHandler linkHandler;
  private final ContentHandler[] handlers;
  private final OutputStream output;
  private final String location;

  public TikaParsingHandler(String location, OutputStream output, ContentHandler mainHandler, TikaLinkContentHandler linkHandler) {
    this.location = location;
    this.mainHandler = mainHandler;
    this.linkHandler = linkHandler;
    this.output = output;
    if (linkHandler != null) {
      handlers = new ContentHandler[]{mainHandler, linkHandler};
    } else {
      handlers = new ContentHandler[]{mainHandler};
    }
  }

  public String getLocation() {
    return location;
  }

  public ContentHandler getMainHandler() {
    return mainHandler;
  }

  public TikaLinkContentHandler getLinkHandler() {
    return linkHandler;
  }

  public OutputStream getOutput() {
    return output;
  }

  @Override
  public final void startPrefixMapping(String prefix, String uri)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.startPrefixMapping(prefix, uri);
    }
  }

  @Override
  public final void endPrefixMapping(String prefix) throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.endPrefixMapping(prefix);
    }
  }

  @Override
  public final void processingInstruction(String target, String data)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.processingInstruction(target, data);
    }
  }

  @Override
  public final void setDocumentLocator(Locator locator) {
    for (ContentHandler handler : handlers) {
      handler.setDocumentLocator(locator);
    }
  }

  @Override
  public final void startDocument() throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.startDocument();
    }
  }

  @Override
  public final void endDocument() throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.endDocument();
    }
  }

  @Override
  public final void startElement(String uri, String localName, String name, Attributes atts)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.startElement(uri, localName, name, atts);
    }
  }

  @Override
  public final void endElement(String uri, String localName, String name)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.endElement(uri, localName, name);
    }
  }

  @Override
  public final void characters(char[] ch, int start, int length)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.characters(ch, start, length);
    }
  }

  @Override
  public final void ignorableWhitespace(char[] ch, int start, int length)
      throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.ignorableWhitespace(ch, start, length);
    }
  }

  @Override
  public final void skippedEntity(String name) throws SAXException {
    for (ContentHandler handler : handlers) {
      handler.skippedEntity(name);
    }
  }
}
