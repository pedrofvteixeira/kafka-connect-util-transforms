package org.pedrofvteixeira.kafka.connect.transformations;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToStringTest {

  static final Logger log = LoggerFactory.getLogger(ToStringTest.class);

  private ToString.Key<SinkRecord> stringKey;
  private ToString.Value<SinkRecord> stringValue;

  @BeforeEach
  public void beforeEach() {
    stringKey = new ToString.Key<>();
    stringValue = new ToString.Value<>();
  }

  @Test
  public void shouldTransformKeyIntoString() {
    log.info("when the SMT is invoked");
    Map<String, Boolean> config = new HashMap<>();
    config.put(ToStringConfig.PARAM_STRIP_MAGIC_BYTE, true);

    final String expected = "{id=1}";

    log.info("then the SMT transforms the key into the intended string");
    assertDoesNotThrow(() -> stringKey.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> stringKey.apply(makeTestSinkRecord()));
    assertNotNull(output);
    assertNotNull(output.key());
    assertEquals(expected, output.key());
  }

  @Test
  public void shouldTransformValueIntoString() {
    log.info("when the SMT is invoked");
    Map<String, Boolean> config = new HashMap<>();
    config.put(ToStringConfig.PARAM_STRIP_MAGIC_BYTE, true);

    final String expected = "{enabled=true}";

    log.info("then the SMT transforms the value into the intended string");
    assertDoesNotThrow(() -> stringValue.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> stringValue.apply(makeTestSinkRecord()));
    assertNotNull(output);
    assertNotNull(output.value());
    assertEquals(expected, output.value());
  }

  private SinkRecord makeTestSinkRecord() {
    Schema keySchema = SchemaBuilder.struct().name("").field("id", STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", "1");

    Schema valueSchema = SchemaBuilder.struct().name("value").field("enabled", BOOLEAN_SCHEMA).build();
    Struct value = new Struct(valueSchema).put("enabled", true);

    return new SinkRecord("test-topic", 1, keySchema,  key, valueSchema, value,
      new Random().nextLong(), new Random().nextLong(), TimestampType.CREATE_TIME);
  }
}