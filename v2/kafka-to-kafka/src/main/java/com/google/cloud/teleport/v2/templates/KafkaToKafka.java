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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.kafka.utils.SslConsumerFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.SslProducerFactoryFn;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;


import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "Kafka_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Kafka",
    description = "A pipeline that writes data to a kafka destination from another kafka source",
    optionsClass = KafkaToKafkaOptions.class,
    flexContainerName = "kafka-to-kafka",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = false)
public class KafkaToKafka {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);

  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();
    KafkaToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToKafkaOptions.class);
    run(options);
  }
  public static PipelineResult run(KafkaToKafkaOptions options) throws IOException {

    if (!Objects.equals(options.getSourceAuthenticationMethod(), "No_Authentication")) {

      checkArgument(
          options.getSourceUsernameSecretId().trim().length() > 0,
          "version id required to access username to GMK");
      checkArgument(
          options.getSourcePasswordSecretId().trim().length() > 0,
          "version id required to access password to authenticate to GMK");
    }

    checkArgument(
        options.getDestinationUsernameSecretId().trim().length() > 0,
        "version id required to access username to GMK");

    checkArgument(

        options.getDestinationPasswordSecretId().trim().length() > 0,
        "version id required to access password to authenticate to GMK");
    if (options.getSourceAuthenticationMethod().equals("SASL_PLAIN")) {
      checkArgument(
          options.getSourceUsernameSecretId().trim().length() > 0,
          "version id required to access username for source Kafka");
      checkArgument(
          options.getSourcePasswordSecretId().trim().length() > 0,
          "version id required to access password for source kafka");
      checkArgument(
          options.getDestinationUsernameSecretId().trim().length() > 0,
          "version id required to access username for destination Kafka");
      checkArgument(
          options.getDestinationPasswordSecretId().trim().length() > 0,
          "version id required to access password for destination kafka");
    } else {
      checkArgument(
          options.getSourceTruststoreLocation().trim().length() > 0,
          "trust store certificate required for ssl authentication");
      checkArgument(
          options.getSourceTruststorePasswordSecretId().trim().length() > 0,
          "trust store password required for accessing truststore");
      checkArgument(
          options.getSourceKeystoreLocation().trim().length() > 0,
          "key store location required for ssl authentication");
      checkArgument(
          options.getSourceKeystorePasswordSecretId().trim().length() > 0,
          "key store password required to access key store");
      checkArgument(
          options.getSourceKeyPasswordSecretId().trim().length() > 0,
          "source key password secret id version required for SSL authentication");
      checkArgument(
          options.getDestinationTruststoreLocation().trim().length() > 0,
          "trust store certificate required for ssl authentication");
      checkArgument(
          options.getDestinationTruststorePasswordSecretId().trim().length() > 0,
          "trust store password required for accessing truststore");
      checkArgument(
          options.getDestinationKeystoreLocation().trim().length() > 0,
          "key store location required for ssl authentication");
      checkArgument(
          options.getDestinationKeystorePasswordSecretId().trim().length() > 0,
          "key store password required to access key store");
      checkArgument(
          options.getDestinationKeyPasswordSecretId().trim().length() > 0,
          "source key password secret id version required for SSL authentication");
    }
    LOG.info("configmap");
    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<byte[], byte[]>> records;
    records =
        pipeline.apply(
            "Read from Kafka",
            KafkaIO.<byte[], byte[]>read()

                .withBootstrapServers("10.128.15.204:9092")
                .withTopic("test")
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withConsumerFactoryFn(new SslConsumerFactoryFn(ConsumerProperties.get(options)))
                .withoutMetadata());
    KafkaIO.Write<byte[], byte[]> kafkaWrite =
        KafkaIO.<byte[], byte[]>write()
            .withBootstrapServers("10.128.15.204:9092")
            .withTopic("outputTopic")
            .withKeySerializer(ByteArraySerializer.class)
            .withValueSerializer(ByteArraySerializer.class);

    records.apply(
        "write messages to kafka",
        kafkaWrite.withProducerFactoryFn(
            new SslProducerFactoryFn(ProducerProperties.get(options))));
    return pipeline.run();
  }
}
