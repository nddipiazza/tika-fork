package org.apache.tika.main;

import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.html.HtmlMapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * HtmlMapper that preserves a few more elements than {@link DefaultHtmlMapper}.
 * Unfortunately that class cannot be subclassed, hence the delegation logic.
 */
public class ExtendedHtmlMapper implements HtmlMapper {
  private static final HtmlMapper MAPPER = DefaultHtmlMapper.INSTANCE;

  public static final HtmlMapper INSTANCE = new ExtendedHtmlMapper();

  private static final Map<String, String> EXT_SAFE_ELEMENTS = new HashMap<String, String>() {{
    put("BR", "br");
    put("HR", "hr");
    put("FORM", "form");
    put("DIV", "div");
    put("SPAN", "span");
    put("B", "b");
    put("STRONG", "strong");
    put("I", "i");
    put("EM", "em");
    put("SMALL", "small");
    put("MARK", "mark");
    put("SUB", "sub");
    put("SUP", "sup");
    put("ABBR", "abbr"); // abbreviation
    put("CITE", "cite");
    put("CODE", "code"); // code
    put("KBD", "kbd"); // sample keyboard input
    put("SAMP", "samp"); // sample output
    put("ARTICLE", "article");
    put("ASIDE", "aside");
    put("DETAILS", "details");
    put("FIGCAPTION", "figcaption");
    put("HEADER", "header");
    put("LABEL", "label");
    put("LEGENG", "legend");
    put("NAV", "nav");
    put("S", "s"); // strike-out
    put("SUMMARY", "summary");
    put("TIME", "time");
    put("B", "b");
    put("B", "b");
    put("B", "b");
  }};

  private static final Set<String> EXT_DISCARDABLE_ELEMENTS = new HashSet<String>() {{

  }};

  private static final Map<String, Set<String>> EXT_SAFE_ATTRIBUTES = new HashMap<String, Set<String>>() {{

  }};

  public String mapSafeElement(String name) {
    String mappedName = EXT_SAFE_ELEMENTS.get(name);
    if (mappedName != null) {
      return mappedName;
    } else {
      return MAPPER.mapSafeElement(name);
    }
  }

  /**
   * Normalizes an attribute name. Assumes that the element name
   * is valid and normalized
   */
  public String mapSafeAttribute(String elementName, String attributeName) {
    Set<String> safeAttrs = EXT_SAFE_ATTRIBUTES.get(elementName);
    if ((safeAttrs != null) && safeAttrs.contains(attributeName)) {
      return attributeName;
    } else {
      return MAPPER.mapSafeAttribute(elementName, attributeName);
    }
  }

  public boolean isDiscardElement(String name) {
    return EXT_DISCARDABLE_ELEMENTS.contains(name) || MAPPER.isDiscardElement(name);
  }

  private static Set<String> attrSet(String... attrs) {
    Set<String> result = new HashSet<String>();
    for (String attr : attrs) {
      result.add(attr);
    }
    return result;
  }
}
