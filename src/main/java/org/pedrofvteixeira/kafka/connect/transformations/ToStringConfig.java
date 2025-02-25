package org.pedrofvteixeira.kafka.connect.transformations;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ToStringConfig extends AbstractConfig {

  public static final String PARAM_STRIP_MAGIC_BYTE = "strip.magic.byte";

  // https://docs.confluent.io/platform/current/connect/devguide.html#configuration-validation
  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    CONFIG_DEF.define(PARAM_STRIP_MAGIC_BYTE, BOOLEAN, false, HIGH, "If we should strip the starting magic byte from the record");
  }

  private boolean stripMagicByte;

  public ToStringConfig(Map<String, ?> settings) {
    super(CONFIG_DEF, settings);
    stripMagicByte = getBoolean(PARAM_STRIP_MAGIC_BYTE) != null ?  getBoolean(PARAM_STRIP_MAGIC_BYTE) : false;
  }

  public boolean isStripMagicByte() {
    return stripMagicByte;
  }
}