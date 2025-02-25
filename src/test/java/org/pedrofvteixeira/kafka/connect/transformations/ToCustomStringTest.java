package org.pedrofvteixeira.kafka.connect.transformations;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToCustomStringTest {

  static final Logger log = LoggerFactory.getLogger(ToCustomStringTest.class);

  private ToCustomString.Key<SinkRecord> customStringKey;
  private ToCustomString.Value<SinkRecord> customStringValue;

  @BeforeEach
  public void beforeEach() {
    customStringKey = new ToCustomString.Key<>();
    customStringValue = new ToCustomString.Value<>();
  }

  @Test
  public void shouldFailWhenFieldsAreNotProvidedInKey() {
    log.info("When the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> configA = new HashMap<>();
    configA.put(ToCustomStringConfig.PARAM_PREFIX, "test");
    configA.put(ToCustomStringConfig.PARAM_DELIMITER, "test");

    Map<String, String> configB = new HashMap<>();
    configB.put(ToCustomStringConfig.PARAM_PREFIX, "test");
    configB.put(ToCustomStringConfig.PARAM_DELIMITER, "#");
    configB.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "");

    log.info("Then the SMT throws an exception");
    assertThrows(DataException.class, () -> customStringKey.configure(configA));
    assertThrows(DataException.class, () -> customStringKey.configure(configB));
  }

  @Test
  public void shouldFailWhenFieldsAreNotProvidedInValue() {
    log.info("When the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> configA = new HashMap<>();
    configA.put(ToCustomStringConfig.PARAM_PREFIX, "test");
    configA.put(ToCustomStringConfig.PARAM_DELIMITER, "#");

    Map<String, String> configB = new HashMap<>();
    configB.put(ToCustomStringConfig.PARAM_PREFIX, "test");
    configB.put(ToCustomStringConfig.PARAM_DELIMITER, "#");
    configB.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "");

    log.info("Then the SMT throws an exception");
    assertThrows(DataException.class, () -> customStringValue.configure(configA));
    assertThrows(DataException.class, () -> customStringValue.configure(configB));
  }

  @Test
  public void shouldTransformKeyIntoCustomString() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> config = new HashMap<>();
    config.put(ToCustomStringConfig.PARAM_PREFIX, "test-prefix");
    config.put(ToCustomStringConfig.PARAM_DELIMITER, "#");
    config.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "first_name,last_name");

    final String firstName = "lIf8aJvbtS";
    final String lastName = "SJg2NMdwdu";
    final String expected = String.format("test-prefix#%s#%s", firstName, lastName);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> customStringKey.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> customStringKey.apply(makeTestKeySinkRecord(firstName, lastName)));
    assertNotNull(output);
    assertNotNull(output.key());
    assertEquals(expected, output.key());
  }

  @Test
  public void shouldTransformValueIntoCustomString() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> config = new HashMap<>();
    config.put(ToCustomStringConfig.PARAM_PREFIX, "test-prefix");
    config.put(ToCustomStringConfig.PARAM_DELIMITER, "#");
    config.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "nationality,citizenship,enabled");

    final String nationality = "lIf8aJvbtS";
    final String citizenship = "SJg2NMdwdu";
    final boolean enabled = new Random().nextBoolean();
    final String expected = String.format("test-prefix#%s#%s#%s", nationality, citizenship, enabled);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> customStringValue.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> customStringValue.apply(makeTestValueSinkRecord(nationality, citizenship, enabled)));
    assertNotNull(output);
    assertNotNull(output.value());
    assertEquals(expected, output.value());
  }

  @Test
  public void shouldTransformKeyIntoCustomStringWithDefaultParameters() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> config = new HashMap<>();
    config.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "first_name,last_name");

    final String firstName = "1KzB0bXRy1";
    final String lastName = "G0FCU2kAHf";
    final String expected = String.format("%s%s%s%s%s", "", "", firstName, "", lastName);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> customStringKey.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> customStringKey.apply(makeTestKeySinkRecord(firstName, lastName)));
    assertNotNull(output);
    assertNotNull(output.key());
    assertEquals(expected, output.key());
  }

  @Test
  public void shouldTransformValueIntoCustomStringWithDefaultParameters() {
    log.info("when the SMT is invoked without providing a comma-separated list of fields");
    Map<String, String> config = new HashMap<>();
    config.put(ToCustomStringConfig.PARAM_COMMA_SEPARATED_FIELDS, "nationality,citizenship,enabled");

    final String nationality = "1KzB0bXRy1";
    final String citizenship = "G0FCU2kAHf";
    final boolean enabled = new Random().nextBoolean();
    final String expected = String.format("%s%s%s%s%s%s%s", "", "", nationality, "", citizenship, "", enabled);

    log.info("then the SMT transforms the key into the intended custom string");
    assertDoesNotThrow(() -> customStringValue.configure(config));
    SinkRecord output = assertDoesNotThrow(() -> customStringValue.apply(makeTestValueSinkRecord(nationality, citizenship, enabled)));
    assertNotNull(output);
    assertNotNull(output.value());
    assertEquals(expected, output.value());
  }

  private SinkRecord makeTestKeySinkRecord(String firstName, String lastName) {
    Schema keySchema = SchemaBuilder.struct().name("key")
      .field("first_name", STRING_SCHEMA)
      .field("middle_name", STRING_SCHEMA)
      .field("last_name", STRING_SCHEMA)
      .build();

    Struct key = new Struct(keySchema)
      .put("first_name", firstName)
      .put("middle_name", "some-middle-name")
      .put("last_name", lastName);

    Schema valueSchema = SchemaBuilder.struct().name("value").field("enabled", BOOLEAN_SCHEMA).build();
    Struct value = new Struct(valueSchema).put("enabled", true);

    return new SinkRecord("test-topic", 1, keySchema,  key, valueSchema, value,
      new Random().nextLong(), new Random().nextLong(), TimestampType.CREATE_TIME);
  }

  private SinkRecord makeTestValueSinkRecord(String nationality, String citizenship, boolean enabled) {
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