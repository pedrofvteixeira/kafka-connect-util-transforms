package org.pteixeira.kafka.connect.transforms;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

public class ToCustomStringConfig extends AbstractConfig {

  public static final String EMPTY = "";

  public static final String PARAM_PREFIX = "prefix";
  public static final String PARAM_DELIMITER = "delimiter";
  public static final String PARAM_COMMA_SEPARATED_FIELDS = "fields";

  private final String prefix;
  private final String delimiter;
  private final String[] fields;

  public static ConfigDef buildConfig() {
    ConfigDef config = new ConfigDef();
    config.define(PARAM_PREFIX, STRING, EMPTY, LOW, "Prefix added to the resulting string");
    config.define(PARAM_DELIMITER, STRING, EMPTY, LOW, "Delimiter between various values within the string");
    config.define(PARAM_COMMA_SEPARATED_FIELDS, STRING, EMPTY, HIGH, "Comma-separated string of fields that make up the string");
    return config;
  }

  public ToCustomStringConfig(Map<String, ?> settings) {
    super(buildConfig(), settings);

    prefix = getString(PARAM_PREFIX);
    delimiter = getString(PARAM_DELIMITER);
    String fieldsStr = getString(PARAM_COMMA_SEPARATED_FIELDS);

    if (fieldsStr == null || fieldsStr.isEmpty()) {
      throw new ConnectException("Missing required configuration: comma-separated string of fields must be provided in 'fields'");
    }

    fields = fieldsStr.split("\\s*,\\s*"); // Split comma-separated list of fields + trim any whitespace
  }

  public String getPrefix() {
    return prefix;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public String[] getFields() {
    return fields;
  }
}
