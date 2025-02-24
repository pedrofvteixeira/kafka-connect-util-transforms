package org.pteixeira.kafka.connect.transforms;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ToString<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(ToString.class);

  private ToStringConfig config;

  public ToString() {} // Plugin class must have a no-args constructor, and cannot be a non-static inner class

  R applyInKey(R r /* record */) {
    if (r == null || r.key() == null) {
      log.warn("ToString: key is null, no toString possible");
      return r;
    }

    String key = config.isStripMagicByte()
      ? r.key().toString().substring(r.key().toString().indexOf("{"))
      : r.key().toString();
    log.debug("ToString: new key is {}", key);
    return r.newRecord(r.topic(), r.kafkaPartition(), STRING_SCHEMA, key, r.valueSchema(), r.value(), r.timestamp());
  }

  R applyInValue(R r /* record */) {
    if (r == null || r.value() == null) {
      log.warn("ToString: value is null, no toString possible");
      return r;
    }

    String value = config.isStripMagicByte()
      ? r.value().toString().substring(r.value().toString().indexOf("{"))
      : r.value().toString();
    log.debug("ToString: new value is {}", value);
    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), STRING_SCHEMA, value, r.timestamp());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new ToStringConfig(configs); // parse provided values for custom parameters
  }

  @Override
  public ConfigDef config() {
    return ToStringConfig.CONFIG_DEF; // lets kafka-connect be aware this transformation accepts custom parameters
  }

  @Override
  public void close() { /* no-op */ }

  public static final class Key<R extends ConnectRecord<R>> extends ToString<R> {

    @Override
    public R apply(R record) {
      return applyInKey(record);
    }
  }

  public static final class Value<R extends ConnectRecord<R>> extends ToString<R> {

    @Override
    public R apply(R record) {
      return applyInValue(record);
    }
  }
}
