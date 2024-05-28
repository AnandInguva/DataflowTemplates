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
package com.google.cloud.teleport.v2.templates;


import static org.apache.hadoop.hdfs.DFSInotifyEventInputStream.LOG;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ConsumerProperties} is a utility class for constructing properties for Kafka
 * consumers. In this case, it is the Kafka source where we read the data from.
 *
 * <p>The {@link ConsumerProperties} class provides a static method to generate consumer properties
 * required for configuring a Kafka consumer. These properties are needed to establish connections
 * to Kafka brokers. They ensure security through SASL authentication. The properties should specify
 * the necessary authentication credentials in order to establish successful connection to the
 * source Kafka.
 */
final class ConsumerProperties {

  private static final Logger LOGG = LoggerFactory.getLogger(ConsumerProperties.class);


  public static ImmutableMap<String, Object> get(KafkaToKafkaOptions options) throws IOException {
    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "10.128.15.204:9092");
    if (options.getSourceAuthenticationMethod().equals("SSL")) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, options.getSourceKeystoreLocation());
      properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          options.getSourceTruststoreLocation());
      properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecretManagerUtils.getSecret(options.getSourceTruststorePasswordSecretId()));
      properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SecretManagerUtils.getSecret(options.getSourceKeystorePasswordSecretId()));
      properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SecretManagerUtils.getSecret(options.getSourceKeyPasswordSecretId()));
      properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }
    if (options.getSourceAuthenticationMethod().equals("SASL_PLAIN")) {
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      //         Note: in other languages, set sasl.username and sasl.password instead.
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required"
              + " username=\'"
              + SecretManagerUtils.getSecret(options.getSourceUsernameSecretId())
              + "\'"
              + " password=\'"
              + SecretManagerUtils.getSecret(options.getSourcePasswordSecretId())
              + "\';");
    }


    return properties.buildOrThrow();
  }
}
