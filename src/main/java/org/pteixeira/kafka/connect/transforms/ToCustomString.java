package org.pteixeira.kafka.connect.transforms;

import static java.util.Optional.ofNullable;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ToCustomString<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(ToCustomString.class);

  private ToCustomStringConfig config;

  public ToCustomString() {} // Plugin class must have a no-args constructor, and cannot be a non-static inner class

  R applyInKey(R r /* record */) {
    if (r == null || r.key() == null) {
      log.warn("ToCustomString: key is null, no custom string possible");
      return r;
    }

    String key = process((Struct) r.key());
    log.debug("ToCustomString: new key is {}", key);
    return r.newRecord(r.topic(), r.kafkaPartition(), STRING_SCHEMA, key, r.valueSchema(), r.value(), r.timestamp());
  }

  R applyInValue(R r /* record */) {
    if (r == null || r.value() == null) {
      log.warn("ToCustomString: value is null, no custom string possible");
      return r;
    }

    String value = process((Struct) r.value());
    log.debug("ToCustomString: new value is {}", value);
    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), STRING_SCHEMA, value, r.timestamp());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new ToCustomStringConfig(configs); // parse provided values for custom parameters
  }

  @Override
  public ConfigDef config() {
    return ToCustomStringConfig.CONFIG_DEF; // lets kafka-connect be aware this transformation accepts custom parameters
  }

  @Override
  public void close() {
  }

  private String process(final Struct struct) {
    final String[] fields = safeSplit(config.getCommaSeparatedFields());
    final StringBuilder sb = new StringBuilder();

    if (isNotEmpty(config.getPrefix())) {
      sb.append(config.getPrefix()).append(config.getDelimiter());
    }

    // Concatenate each specified field using the delimiter
    Arrays.stream(fields)
        .filter(this::isNotEmpty)
        .forEach(field -> ofNullable(struct.get(field))
        .ifPresent(val -> sb.append(val.toString().trim()).append(config.getDelimiter())));

    String customString = sb.toString();
    return customString.endsWith(config.getDelimiter())
      ? customString.substring(0, customString.lastIndexOf(config.getDelimiter()))
      : customString;
  }

  private String[] safeSplit(String commaSeparatedString) {
    return commaSeparatedString == null ? new String[]{} : commaSeparatedString.split("\\s*,\\s*");
  }

  private boolean isNotEmpty(String s) {
    return s != null && !s.isEmpty();
  }

  public static final class Key<R extends ConnectRecord<R>> extends ToCustomString<R> {

    @Override
    public R apply(R record) {
      return applyInKey(record);
    }
  }

  public static final class Value<R extends ConnectRecord<R>> extends ToCustomString<R> {

    @Override
    public R apply(R record) {
      return applyInValue(record);
    }
  }
}
