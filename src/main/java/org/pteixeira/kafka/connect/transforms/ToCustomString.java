package org.pteixeira.kafka.connect.transforms;

import static java.util.Optional.ofNullable;

import java.util.Arrays;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

public class ToCustomString {

  private final String prefix;
  private final String delimiter;
  private final String[] fields;

  public ToCustomString(final String prefix, final String delimiter, final String[] fields) {
    this.prefix = prefix;
    this.delimiter = delimiter;
    this.fields = fields;

    if (this.prefix == null) {
      throw new ConnectException("Missing required configuration: prefix");
    }
    if (this.delimiter == null) {
      throw new ConnectException("Missing required configuration: delimiter");
    }
    if (this.fields == null || this.fields.length == 0) {
      throw new ConnectException("Missing required configuration: fields");
    }
  }

  String process(final Struct struct) {
    final StringBuilder sb = new StringBuilder(prefix).append(delimiter);

    // Concatenate each specified field using the delimiter
    Arrays.stream(fields)
      .forEach(field -> ofNullable(struct.get(field))
      .ifPresent(val -> sb.append(val.toString().trim()).append(delimiter)));

    String customString = sb.toString();
    return customString.endsWith(delimiter)
      ? customString.substring(0, customString.lastIndexOf(delimiter))
      : customString;
  }
}
