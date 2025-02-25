package org.pedrofvteixeira.kafka.connect.transformations;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;

public class ToCustomStringConfig extends AbstractConfig {

  public static final String PARAM_PREFIX = "prefix";
  public static final String PARAM_DELIMITER = "delimiter";
  public static final String PARAM_COMMA_SEPARATED_FIELDS = "fields";

  // https://docs.confluent.io/platform/current/connect/devguide.html#configuration-validation
  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    CONFIG_DEF.define(PARAM_COMMA_SEPARATED_FIELDS, STRING, "", HIGH, "comma-separated list of fields used to compose the custom string");
    CONFIG_DEF.define(PARAM_PREFIX, STRING, "", LOW, "A value to prepend to the custom string");
    CONFIG_DEF.define(PARAM_DELIMITER, STRING, "", LOW, "A delimiter between prefix and each of the fields in the list");
  }

  private String prefix;
  private String delimiter;
  private String commaSeparatedFields;

  public ToCustomStringConfig(Map<String, ?> settings) {
    super(CONFIG_DEF, settings);

    prefix = getString(PARAM_PREFIX);
    delimiter = getString(PARAM_DELIMITER);
    commaSeparatedFields = getString(PARAM_COMMA_SEPARATED_FIELDS);

    if (commaSeparatedFields == null || commaSeparatedFields.isEmpty()) {
      throw new DataException("Missing required 'fields'");
    }
  }

  public String getPrefix() {
    return prefix;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public String getCommaSeparatedFields() {
    return commaSeparatedFields;
  }
}