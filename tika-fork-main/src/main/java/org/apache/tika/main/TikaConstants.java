package org.apache.tika.main;

import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;

public class TikaConstants {
  public static final String TYPE = "tika-parser";

  public static final String defaultInputEncoding = "UTF-8";
  public static final String defaultOutputEncoding = "UTF-8";

  public static final String BODY_FIELD = "body";
  public static final String CONTAINER_FIELD = "container";
  public static final String RESOURCE_FIELD = "resource_name";
  public static final String PARSE_TIME_FIELD = "parsing_time";

  public static final String INVALID_LAST_MODIFIED_DATE = "0002-11-30T00:00:00Z";

  public static final String RESOURCE_SEPARATOR = "#";
  public final static Property WRITE_LIMIT_REACHED =
      Property.internalBoolean(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX + "write_limit_reached");
  public final static Property EMBEDDED_RESOURCE_LIMIT_REACHED =
      Property.internalBoolean(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX + "embedded_resource_limit_reached");
  //move this to TikaCoreProperties?
  public final static Property EMBEDDED_RESOURCE_PATH =
      Property.internalText(TikaCoreProperties.TIKA_META_PREFIX + "embedded_resource_path");
}
