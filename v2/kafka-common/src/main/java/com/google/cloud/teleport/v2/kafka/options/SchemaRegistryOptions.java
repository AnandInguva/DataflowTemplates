/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.kafka.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.SchemaFormat;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface SchemaRegistryOptions extends PipelineOptions {
  @TemplateParameter.Enum(
      order = 1,
      name = "messageFormat",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT),
        @TemplateParameter.TemplateEnumOption(MessageFormatConstants.AVRO_BINARY_ENCODING),
        @TemplateParameter.TemplateEnumOption(MessageFormatConstants.JSON)
      },
      description = "Kafka Message Format",
      helpText =
          "The format of the Kafka messages to read. The supported values are AVRO_CONFLUENT_WIRE_FORMAT (Confluent Schema Registry encoded Avro), AVRO_BINARY_ENCODING (Plain binary Avro), and JSON.")
  @Default.String(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
  String getMessageFormat();

  void setMessageFormat(String value);

  @TemplateParameter.Enum(
      order = 2,
      name = "schemaSource",
      groupName = "Source",
      parentName = "messageFormat",
      parentTriggerValues = {MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT},
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(SchemaFormat.SCHEMA_REGISTRY),
        @TemplateParameter.TemplateEnumOption(SchemaFormat.SINGLE_SCHEMA_FILE),
      },
      description = "Schema Source",
      optional = true,
      helpText =
          "The Kafka schema source. It can be provided as SINGLE_SCHEMA_FILE or SCHEMA_REGISTRY. "
              + "If SINGLE_SCHEMA_FILE is specified, all the kafka messages should have the schema mentioned in the avro schema file. "
              + "If SCHEMA_REGISTRY is specified, the kafka messages can have either a single schema or multiple schemas that are registered"
              + "in the schema registry.")
  @Default.String(SchemaFormat.SINGLE_SCHEMA_FILE)
  String getSchemaSource();

  void setSchemaSource(String value);

  @TemplateParameter.GcsReadFile(
      order = 3,
      groupName = "Source",
      parentName = "schemaSource",
      parentTriggerValues = {SchemaFormat.SINGLE_SCHEMA_FILE},
      description = "Cloud Storage path to the Avro schema file",
      optional = true,
      helpText =
          "The Google Cloud Storage path to the single Avro schema file used to "
              + "decode all of the messages in a topic.")
  @Default.String("")
  String getConfluentAvroSchemaPath();

  void setConfluentAvroSchemaPath(String schemaPath);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Source",
      parentName = "schemaSource",
      parentTriggerValues = {SchemaFormat.SCHEMA_REGISTRY},
      description = "Schema Registry Connection URL",
      optional = true,
      helpText =
          "The URL for the Confluent Schema Registry instance used to manage Avro schemas"
              + " for message decoding.")
  @Default.String("")
  String getSchemaRegistryConnectionUrl();

  void setSchemaRegistryConnectionUrl(String schemaRegistryConnectionUrl);

  @TemplateParameter.GcsReadFile(
      order = 5,
      groupName = "Source",
      parentName = "messageFormat",
      parentTriggerValues = {MessageFormatConstants.AVRO_BINARY_ENCODING},
      description = "Cloud Storage path to the Avro schema file",
      optional = true,
      helpText =
          "The Google Cloud Storage path to the Avro schema file used to decode binary-encoded Avro messages.")
  @Default.String("")
  String getBinaryAvroSchemaPath();

  void setBinaryAvroSchemaPath(String schemaPath);

  @TemplateParameter.Enum(
      order = 6,
      name = "schemaRegistryAuthenticationMode",
      groupName = "Source",
      parentName = "schemaFormat",
      parentTriggerValues = {SchemaFormat.SCHEMA_REGISTRY},
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.TLS),
      },
      optional = true,
      description = "Authentication Mode",
      helpText = "Schema Registry authentication mode. Can be NONE or TLS.")
  @Default.String(KafkaAuthenticationMethod.NONE)
  String getSchemaRegistryAuthenticationMode();

  void setSchemaRegistryAuthenticationMode(String value);

  @TemplateParameter.GcsReadFile(
      order = 7,
      parentName = "schemaRegistryAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      optional = true,
      description = "Truststore File Location",
      helpText =
          "Location of the SSL certificate where the trust store for authentication to Schema Registry are stored.",
      example = "/your-bucket/truststore.jks")
  String getSchemaRegistryTruststoreLocation();

  void setSchemaRegistryTruststoreLocation(String truststoreLocation);

  @TemplateParameter.Text(
      order = 8,
      parentName = "schemaRegistryAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      optional = true,
      description = "Truststore Password",
      helpText =
          "SecretId in secret manager where the password to access secret in truststore is stored.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSchemaRegistryTruststorePasswordSecretId();

  void setSchemaRegistryTruststorePasswordSecretId(String truststorePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 9,
      parentName = "schemaRegistryAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      optional = true,
      description = "Keystore File Location",
      helpText = "Keystore location that contains the SSL certificate and private key.",
      example = "/your-bucket/keystore.jks")
  String getSchemaRegistryKeystoreLocation();

  void setSchemaRegistryKeystoreLocation(String keystoreLocation);

  @TemplateParameter.Text(
      order = 10,
      parentName = "schemaRegistryAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      optional = true,
      description = "Keystore Password",
      helpText = "SecretId in secret manager where the password to access the keystore file",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSchemaRegistryKeystorePasswordSecretId();

  void setSchemaRegistryKeystorePasswordSecretId(String keystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      parentName = "schemaRegistryAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      optional = true,
      description = "Private Key Password",
      helpText =
          "SecretId of password required to access the client's private key stored within the keystore",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSchemaRegistryKeyPasswordSecretId();

  void setSchemaRegistryKeyPasswordSecretId(String keyPasswordSecretId);
}
