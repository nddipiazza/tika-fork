package org.apache.tika.fork;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/** This class is a copy of Tika LinkContentHandler plus our extensions. The original class
 * was impossible to extend. */
public class TikaLinkContentHandler extends DefaultHandler {

  /**
   * Stack of link builders, one for each level of nested links currently being
   * processed. A usual case of a nested link would be a hyperlinked image (
   * <code>&a href="..."&gt;&lt;img src="..."&gt;&lt;&gt;</code>), but it's
   * possible (though unlikely) for also other kinds of nesting to occur.
   */
  private final LinkedList<LinkBuilder> builderStack = new LinkedList<LinkBuilder>();
  
  private static final Set<String> elements = new HashSet<String>(Arrays.asList(
          "a", "img", "input", "link", "form", "embed", "source",
          "track", "object", "frame", "iframe", "area", "script"));

  /** Collected links */
  private final List<Link> links = new ArrayList<Link>();
  private final String baseUri;

  /** Whether to collapse whitespace in anchor text */
  private boolean collapseWhitespaceInAnchor;

  /**
   * Default constructor
   */
  public TikaLinkContentHandler(String baseUri) {
    this(baseUri, false);
  }

  /**
   * Default constructor
   *
   * @param baseUri The base URI
   * @param collapseWhitespaceInAnchor set if whitespace in the anchor should be collapsed
   */
  public TikaLinkContentHandler(String baseUri, boolean collapseWhitespaceInAnchor) {
    super();

    this.collapseWhitespaceInAnchor = collapseWhitespaceInAnchor;
    this.baseUri = baseUri;
  }

  /**
   * Returns the list of collected links.
   *
   * @return collected links
   */
  public List<Link> getLinks() {
    return links;
  }

  // -------------------------------------------------------< ContentHandler>

  @Override
  public void startElement(String uri, String local, String name,
          Attributes attributes) {
    if (XHTML.equals(uri)) {
      if ("a".equals(local)) {
        LinkBuilder builder = new LinkBuilder("a");
        builder.setURI(attributes.getValue("", "href"));
        builder.setTitle(attributes.getValue("", "title"));
        builder.setRel(attributes.getValue("", "rel"));
        builder.setParam("id", attributes.getValue("", "id"));
        builderStack.addFirst(builder);
      } else if ("img".equals(local)) {
        LinkBuilder builder = new LinkBuilder("img");
        builder.setURI(attributes.getValue("", "src"));
        builder.setTitle(attributes.getValue("", "title"));
        builder.setRel(attributes.getValue("", "rel"));
        builder.setParam("alt", attributes.getValue("", "alt"));
        builderStack.addFirst(builder);

        String alt = attributes.getValue("", "alt");
        if (alt != null) {
          char[] ch = alt.toCharArray();
          characters(ch, 0, ch.length);
        }
      } else if ("input".equals(local)) {
        if (!"image".equalsIgnoreCase(attributes.getValue("type"))) {
          return;
        }
        LinkBuilder builder = new LinkBuilder("input");
        builder.setURI(attributes.getValue("", "src"));
        builder.setTitle(attributes.getValue("", "name"));
        builder.setParam("alt", attributes.getValue("", "alt"));
        builderStack.addFirst(builder);

        String alt = attributes.getValue("", "alt");
        if (alt != null) {
          char[] ch = alt.toCharArray();
          characters(ch, 0, ch.length);
        }
      } else if ("link".equals(local)) {
        LinkBuilder builder = new LinkBuilder("link");
        builder.setURI(attributes.getValue("", "href"));
        builder.setTitle(attributes.getValue("", "title"));
        builder.setRel(attributes.getValue("", "rel"));
        builder.setParam("type", attributes.getValue("", "type"));
        builderStack.addFirst(builder);
      } else if ("form".equals(local)) { // only forms that do GET
        String method = attributes.getValue("", "method");
        if (method != null && !"get".equalsIgnoreCase(method)) {
          return;
        }          
        LinkBuilder builder = new LinkBuilder("form");
        builder.setURI(attributes.getValue("", "action"));
        builder.setTitle(attributes.getValue("", "name"));
        builder.setParam("target", attributes.getValue("", "target"));
        builderStack.addFirst(builder);
      } else if ("embed".equals(local)) {
        LinkBuilder builder = new LinkBuilder("embed");
        builder.setURI(attributes.getValue("", "src"));
        builder.setTitle(attributes.getValue("", "name"));
        builder.setParam("type", attributes.getValue("", "type"));
        builder.setParam("height", attributes.getValue("", "height"));
        builder.setParam("width", attributes.getValue("", "width"));
        builderStack.addFirst(builder);
      } else if ("source".equals(local)) {
        LinkBuilder builder = new LinkBuilder("source");
        builder.setURI(attributes.getValue("", "src"));
        builder.setParam("type", attributes.getValue("", "type"));
        builder.setParam("media", attributes.getValue("", "media"));
        builderStack.addFirst(builder);
      } else if ("track".equals(local)) {
        LinkBuilder builder = new LinkBuilder("track");
        builder.setURI(attributes.getValue("", "src"));
        builder.setTitle(attributes.getValue("", "label"));
        builder.setParam("kind", attributes.getValue("", "kind"));
        builder.setParam("srclang", attributes.getValue("", "srclang"));
        builderStack.addFirst(builder);
      } else if ("object".equals(local)) {
        LinkBuilder builder = new LinkBuilder("object");
        builder.setURI(attributes.getValue("", "data"));
        builder.setParam("type", attributes.getValue("", "type"));
        builder.setParam("height", attributes.getValue("", "height"));
        builder.setParam("width", attributes.getValue("", "width"));
        builderStack.addFirst(builder);
      } else if ("frame".equals(local) || "iframe".equals(local)) {
        LinkBuilder builder = new LinkBuilder(local);
        builder.setURI(attributes.getValue("", "src"));
        builder.setTitle(attributes.getValue("", "name"));
        builderStack.addFirst(builder);
      } else if ("area".equals(local)) {
        LinkBuilder builder = new LinkBuilder("area");
        builder.setURI(attributes.getValue("", "href"));
        builder.setParam("shape", attributes.getValue("", "shape"));
        builder.setParam("coords", attributes.getValue("", "coords"));
        builder.setParam("alt", attributes.getValue("", "alt"));
        String alt = attributes.getValue("", "alt");
        if (alt != null) {
          char[] ch = alt.toCharArray();
          characters(ch, 0, ch.length);
        }
        builderStack.addFirst(builder);
      } else if ("script".equals(local)) {
        LinkBuilder builder = new LinkBuilder("script");
        builder.setURI(attributes.getValue("", "src"));
        builder.setParam("type", attributes.getValue("", "type"));
        builderStack.addFirst(builder);
      }
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    for (LinkBuilder builder : builderStack) {
      builder.characters(ch, start, length);
    }
  }

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) {
    characters(ch, start, length);
  }

  @Override
  public void endElement(String uri, String local, String name) {
    if (XHTML.equals(uri)) {
      if (elements.contains(local)) {
        if (!builderStack.isEmpty()) {
          links.add(builderStack.removeFirst()
              .getLink(baseUri, collapseWhitespaceInAnchor));
        }
      }
    }
  }
}

class LinkBuilder {

  private final String type;

  private String uri = "";

  private String title = "";

  private String rel = "";

  private final StringBuilder text = new StringBuilder();
  private final Map<String,Object> params = new HashMap<String,Object>();

  public LinkBuilder(String type) {
    this.type = type;
  }

  public void setURI(String uri) {
    if (uri != null) {
      this.uri = uri;
    } else {
      this.uri = "";
    }
  }

  public void setTitle(String title) {
    if (title != null) {
      this.title = title;
    } else {
      this.title = "";
    }
  }

  public void setRel(String rel) {
    if (rel != null) {
      this.rel = rel;
    } else {
      this.rel = "";
    }
  }
  
  public void setParam(String name, Object value) {
    if (value != null) {
      params.put(name, value);
    } else {
      params.remove(name);
    }
  }

  public void characters(char[] ch, int offset, int length) {
    text.append(ch, offset, length);
  }

  public Link getLink(String baseUri) {
    return getLink(baseUri, false);
  }

  public Link getLink(String baseUri, boolean collapseWhitespace) {
    String anchor = text.toString();

    if (collapseWhitespace) {
      anchor = anchor.replaceAll("\\s+", " ").trim();
    }

    return new Link(type, baseUri, uri, title, anchor, rel, params);
  }

}
