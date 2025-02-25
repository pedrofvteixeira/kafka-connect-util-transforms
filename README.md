### How-to 

Run `mvn clean package`

Confluent's `kafka-connect-maven-plugin` will generate a `manifest.json`, package all classes and create a .zip compliant with the expected connector plugin structure and filename, [as defined in Confluent's documentation](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-qs.html#packaging-a-custom-connector) 

The zipped plugin will be in `/target/components/packages`