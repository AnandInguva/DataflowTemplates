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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

/**
 * 1. Test the Confluent wire format implementation with Schema registry. 2. Test the Confluent wire
 * format implementation without schema registry 3. Test the JSON implementation.
 */

/** Integration test for {@link KafkaToGcs2} */
@RunWith(JUnit4.class)
//@TemplateIntegrationTest(KafkaToGcs2.class)
//@Category(TemplateIntegrationTest.class)
public class KafkaToGcsIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToGcsIT.class);
  private KafkaResourceManager kafkaResourceManager;
  private Schema avroSchema;
  private Schema avroSchema1;
  private Schema avroSchema2;

  @Before
  public void setup() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    URL avroSchemaResource = Resources.getResource("KafkaToBigQueryAvroIT/avro_schema.avsc");
    gcsClient.uploadArtifact("schema.avsc", avroSchemaResource.getPath());
    avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaResource.openStream());

    URL avroSchemaResource1 = Resources.getResource("KafkaToGcsIT/avro_schema1.avsc");
    gcsClient.uploadArtifact("avro_schema1.avsc", avroSchemaResource1.getPath());
    avroSchema1 = new org.apache.avro.Schema.Parser().parse(avroSchemaResource1.openStream());


    URL avroSchemaResource2 = Resources.getResource("KafkaToGcsIT/avro_schema2.avsc");
    gcsClient.uploadArtifact("avro_schema2.avsc", avroSchemaResource2.getPath());
    avroSchema2 = new org.apache.avro.Schema.Parser().parse(avroSchemaResource2.openStream());

  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager);
  }

  @Test
  public void testKafkaToGcs() throws IOException {
    kafkaToGcs(Function.identity());
  }

  public void kafkaToGcs(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    String topicName = kafkaResourceManager.createTopic(testName, 5);
    String bootStrapServers = kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "bootstrapServers", bootStrapServers)
                .addParameter("inputTopics", topicName));

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());
    for (int i = 1; i <= 10; i++) {
      publish(kafkaProducer, topicName, i + "1", "{\"id\": " + i + "1, \"name\": \"Dataflow\"}");
      publish(kafkaProducer, topicName, i + "2", "{\"id\": " + i + "2, \"name\": \"Pub/Sub\"}");
      // Invalid schema
      publish(
          kafkaProducer, topicName, i + "3", "{\"id\": " + i + "3, \"description\": \"Pub/Sub\"}");

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void kafkaToGcsAvroConfluentWireFormat(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, RestClientException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "bootstrapServers",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                .addParameter("inputTopics", topicName)
                .addParameter("schemaPath", getGcsPath("schema.avsc")));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    registryClient.register(topicName + "-value", avroSchema, 1, 1);

    KafkaProducer<String, Object> kafkaProducer =
        kafkaResourceManager.buildProducer(
            new StringSerializer(), new KafkaAvroSerializer(registryClient));

    for (int i = 1; i <= 10; i++) {
      GenericRecord dataflow = createRecord(Integer.parseInt(i + "1"), "Dataflow", null);
      publish(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.parseInt(i + "2"), "Pub/Sub", null);
      publish(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid = createRecord(Integer.parseInt(i + "3"), "InvalidNameTooLong", null);
      publish(kafkaProducer, topicName, i + "3", invalid);
    }
  }

  @Test
  public void kafkaToGcsAvroConfluentWireFormatWithSchemaRegistryURL(Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder> paramsAdder)
      throws RestClientException, IOException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);
//    String bootStrapServers = kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");
    Map<String, Object> props = new HashMap<>();

    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-scope");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaResourceManager.getBootstrapServers());

    // Register two schemas in a same subject.
    SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("test-scope");
    schemaRegistryClient.register("subject", avroSchema1, 1, 1);
    schemaRegistryClient.register("subject", avroSchema2, 1, 2);


    KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(props,
            new StringSerializer(), new KafkaAvroSerializer());


    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
                PipelineLauncher.LaunchConfig.builder(testName, specPath)
                        .addParameter(
                                    "bootstrapServers",
                                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                          .addParameter("inputTopics", topicName)
                          .addParameter("messageFormat", "CONFLUENT_WIRE_FORMAT")
                          .addParameter("avroSchemaPath", getGcsPath("schema.avsc"))
                        .addParameter("offset", "earliest")
                        .addParameter("outputDirectory", "gs://anandinguva--test-1/testing")
                        .addParameter("schemaRegistryURL", "mock://test-scope")
        );
    
    for (int i = 1; i < 10; i++) {
      GenericRecord genericRecord1 = createRecord(i, "Dataflow", null);
      GenericRecord genericRecord2 = createRecord(i, "Pub/Sub", String.format("Record with id: %s", i));
      publish(kafkaProducer, topicName, String.valueOf(i), genericRecord1);
      publish(kafkaProducer, topicName, String.valueOf(i), genericRecord2);
    }

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
  }

  private void publish(
      KafkaProducer<String, Object> producer, String topicName, String key, GenericRecord value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }

  private void publish(
      KafkaProducer<String, String> producer, String topicName, String key, String value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }

  private GenericRecord createRecord(int id, String name, String description) {
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(avroSchema)
            .set("productId", id)
            .set("productName", name);
    if (!description.isEmpty()) {
      genericRecordBuilder.set("productDescription", description);
    }
    return genericRecordBuilder.build();
  }
}
