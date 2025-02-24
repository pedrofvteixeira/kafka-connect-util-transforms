package org.pteixeira.kafka.connect.transforms;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueToCustomString<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(ValueToCustomString.class);

  private ToCustomString toCustomString;

  public ValueToCustomString() { /* Plugin class must have a no-args constructor, and cannot be a non-static inner class */ }

  @Override
  public R apply(R r /* record */) {
    if (r == null || r.key() == null) {
      log.warn("ValueToCustomString: value is null, no custom string will be built");
      return r;
    }

    var value = toCustomString.process((Struct) r.value());
    log.debug("ValueToCustomString: new value is " + value);

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), STRING_SCHEMA, value, r.timestamp());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    ToCustomStringConfig config = new ToCustomStringConfig(configs); // parse provided values for custom parameters
    this.toCustomString = new ToCustomString(config.getPrefix(), config.getDelimiter(), config.getFields());
  }

  @Override
  public ConfigDef config() {
    return ToCustomStringConfig.buildConfig(); // lets kafka-connect be aware this transformation accepts custom parameters
  }

  @Override
  public void close() {
  }
}
