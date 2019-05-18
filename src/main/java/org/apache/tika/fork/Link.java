package org.apache.tika.fork;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Link {
  
  public String type;
  public String sourceUri;
  public String targetUri;
  public String title;
  public String anchor;
  public String rel;
  public final Map<String,Object> params = new HashMap<String,Object>();
  
  @JsonCreator
  public Link(
          @JsonProperty("type") String type,
          @JsonProperty("sourceUri") String sourceUri,
          @JsonProperty("targetUri") String targetUri,
          @JsonProperty("title") String title,
          @JsonProperty("anchor") String anchor,
          @JsonProperty("rel") String rel,
          @JsonProperty("params") Map<String,Object> params) {
    this.type = type;
    this.sourceUri = sourceUri;
    this.targetUri = targetUri;
    this.title = title;
    this.anchor = anchor;
    this.rel = rel;
    if (params != null && !params.isEmpty()) {
      this.params.putAll(params);
    }
  }
  
  public Map<String,Object> toMap() {
    Map<String,Object> map = new HashMap<>();
    if (type != null && !type.trim().isEmpty()) {
      map.put("type", type);
    }
    if (sourceUri != null && !sourceUri.trim().isEmpty()) {
      map.put("sourceUri", sourceUri);
    }
    if (targetUri != null && !targetUri.trim().isEmpty()) {
      map.put("targetUri", targetUri);
    }
    if (title != null && !title.trim().isEmpty()) {
      map.put("title", title);
    }
    if (rel != null && !rel.trim().isEmpty()) {
      map.put("rel", rel);
    }
    if (!params.isEmpty()) {
      map.put("params", params);
    }
    if (anchor != null && !anchor.trim().isEmpty()) {
      map.put("anchor", anchor);
    }
    return map;
  }

  @Override
  public String toString() {
    return "Link [type=" + type + ", sourceUri=" + sourceUri + ", targetUri="
            + targetUri + ", title=" + title + ", anchor=" + anchor + ", rel="
            + rel + ", params=" + params + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Link)) {
      return false;
    }

    Link link = (Link) o;

    if (type != null ? !type.equals(link.type) : link.type != null) {
      return false;
    }
    if (sourceUri != null ? !sourceUri.equals(link.sourceUri) : link.sourceUri != null) {
      return false;
    }
    if (targetUri != null ? !targetUri.equals(link.targetUri) : link.targetUri != null) {
      return false;
    }
    if (title != null ? !title.equals(link.title) : link.title != null) {
      return false;
    }
    if (anchor != null ? !anchor.equals(link.anchor) : link.anchor != null) {
      return false;
    }
    if (rel != null ? !rel.equals(link.rel) : link.rel != null) {
      return false;
    }
    return params != null ? params.equals(link.params) : link.params == null;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (sourceUri != null ? sourceUri.hashCode() : 0);
    result = 31 * result + (targetUri != null ? targetUri.hashCode() : 0);
    result = 31 * result + (title != null ? title.hashCode() : 0);
    result = 31 * result + (anchor != null ? anchor.hashCode() : 0);
    result = 31 * result + (rel != null ? rel.hashCode() : 0);
    result = 31 * result + (params != null ? params.hashCode() : 0);
    return result;
  }
}
