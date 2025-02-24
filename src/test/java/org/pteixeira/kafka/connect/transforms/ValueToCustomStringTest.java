package org.pteixeira.kafka.connect.transforms;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.pteixeira.kafka.connect.transforms.ToCustomStringConfig.EMPTY;
import static org.pteixeira.kafka.connect.transforms.ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS;
import static org.pteixeira.kafka.connect.transforms.ToCustomStringConfig.PARAM_DELIMITER;
import static org.pteixeira.kafka.connect.transforms.ToCustomStringConfig.PARAM_PREFIX;

import java.util.Map;
import java.util.Random;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueToCustomStringTest {

  static final Logger log = LoggerFactory.getLogger(ValueToCustomStringTest.class);

  private ValueToCustomString<SinkRecord> smt;

  @BeforeEach
  public void beforeEach() {
    smt = new ValueToCustomString<>();
  }

  @Test
  public void shouldFailWhenFieldsAreNotProvided() {
    log.info("When the SMT is invoked without providing a comma-separated list of fields");
    var configA = Map.of(
      PARAM_PREFIX, "test",
      PARAM_DELIMITER, "#"
    );

    var configB = Map.of(
      PARAM_PREFIX, "test",
      PARAM_DELIMITER, "#",
      PARAM_COMMA_SEPARATED_FIELDS, ""
    );

    log.info("Then the SMT throws an exception");
    assertThrows(ConnectException.class, () -> smt.configure(configA));
    assertThrows(ConnectException.class, () -> smt.configure(configB));
  }

  @Test
  public void shouldTransformKeyIntoCustomString() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    var config = Map.of(
      PARAM_PREFIX, "test-prefix",
      PARAM_DELIMITER, "#",
      PARAM_COMMA_SEPARATED_FIELDS, "nationality,citizenship,enabled"
    );

    final var nationality = "lIf8aJvbtS";
    final var citizenship = "SJg2NMdwdu";
    final var enabled = new Random().nextBoolean();
    final var expected = String.format("test-prefix#%s#%s#%s", nationality, citizenship, enabled);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> smt.configure(config));
    final var output = assertDoesNotThrow(() -> smt.apply(makeTestSinkRecord(nationality, citizenship, enabled)));
    assertNotNull(output);
    assertNotNull(output.value());
    assertEquals(expected, output.value());
  }

  @Test
  public void shouldTransformKeyIntoCustomStringWithDefaultParameters() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    var config = Map.of(
      PARAM_COMMA_SEPARATED_FIELDS, "nationality,citizenship,enabled"
    );

    final var nationality = "1KzB0bXRy1";
    final var citizenship = "G0FCU2kAHf";
    final var enabled = new Random().nextBoolean();
    final var expected = String.format("%s%s%s%s%s%s%s", EMPTY, EMPTY, nationality, EMPTY, citizenship, EMPTY, enabled);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> smt.configure(config));
    final var output = assertDoesNotThrow(() -> smt.apply(makeTestSinkRecord(nationality, citizenship, enabled)));
    assertNotNull(output);
    assertNotNull(output.value());
    assertEquals(expected, output.value());
  }

  private SinkRecord makeTestSinkRecord(String nationality, String citizenship, boolean enabled) {
    Schema keySchema = SchemaBuilder.struct().name("key").field("id", INT32_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", new Random().nextInt());

    Schema valueSchema = SchemaBuilder.struct().name("value")
      .field("nationality", STRING_SCHEMA)
      .field("citizenship", STRING_SCHEMA)
      .field("enabled", BOOLEAN_SCHEMA)
      .build();

    Struct value = new Struct(valueSchema)
      .put("nationality", nationality)
      .put("citizenship", citizenship)
      .put("enabled", enabled);

    return new SinkRecord("test-topic", 1, keySchema,  key, valueSchema, value,
      new Random().nextLong(), new Random().nextLong(), TimestampType.CREATE_TIME);
  }
}
