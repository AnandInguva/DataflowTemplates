/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.options.KafkaToGcsOptions;
import com.google.cloud.teleport.v2.transforms.FileFormatFactory;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.com.google.common.io.Resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** Test class for {@link KafkaToGCS}. */
@RunWith(JUnit4.class)
public class KafkaToGCSTest {

  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  /** Tests parseDuration() with a valid value. */
  @Test
  public void testParseDuration() {
    String value = "2m";

    Duration duration = DurationUtils.parseDuration(value);
    assertThat(duration, is(notNullValue()));
    assertThat(duration.getStandardMinutes(), is(equalTo(2L)));
  }

  /** Tests parseDuration() with a null value. */
  @Test
  public void testParseDurationNull() {
    String value = null;

    exception.expect(NullPointerException.class);
    exception.expectMessage("The specified duration must be a non-null value!");
    DurationUtils.parseDuration(value);
  }

  /** Tests parseDuration() when given a negative value. */
  @Test
  public void testParseDurationNegative() {
    String value = "-2m";

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("The window duration must be greater than 0!");
    DurationUtils.parseDuration(value);
  }

  /**
   * Test whether {@link FileFormatFactory} maps the output file format to the transform to be
   * carried out. And throws illegal argument exception if invalid file format is passed.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFileFormatFactoryInvalid() {

    // Create the test input.
    final String key = "Name";
    final String value = "Generic";
    final KV<String, String> message = KV.of(key, value);

    final String outputDirectory = "gs://bucket_name/path/to/output-location";
    final String outputFileFormat = "json".toUpperCase();
    final String outputFilenamePrefix = "output";
    final Integer numShards = 1;
    final String tempOutputDirectory = "gs://bucket_name/path/to/temp-location";

    KafkaToGcsOptions options = PipelineOptionsFactory.create().as(KafkaToGcsOptions.class);

    options.setOutputFileFormat(outputFileFormat);
    options.setOutputDirectory(outputDirectory);
    options.setOutputFilenamePrefix(outputFilenamePrefix);
    options.setNumShards(numShards);
    options.setTempLocation(tempOutputDirectory);

    exception.expect(IllegalArgumentException.class);

    TestPipeline pipeline = TestPipeline.create();
    PCollection<KV<String, String>> records =
        pipeline.apply(
            "CreateInput",
            Create.of(message).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    records.apply("WriteToGCS", FileFormatFactory.newBuilder().setOptions(options).build());

    // Run the pipeline.
    pipeline.run();
  }

  /** Tests testFileFormat() with a invalid file format. */
  @Test
  public void testFileFormatInvalid() {
    String fileFormat = "blah".toUpperCase();

    Boolean status = WriteToGCSUtility.isValidFileFormat(fileFormat);
    assertThat(status, is(notNullValue()));
    assertThat(status, is(equalTo(false)));
  }

  public static KafkaProducer<String, GenericRecord> createStringProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://my-scope");
    return new KafkaProducer<>(props);
  }

  @Test
  public void testSchemaRegistry() throws IOException, RestClientException, ExecutionException, InterruptedException {
    String bootStrapServer = "https://localhost:9092";
    String topic = "quickstart-events";
    SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("my-scope");

    String schemaPath1 = "/Users/anandinguva/Desktop/projects/DataflowTemplates/v2/kafka-to-gcs/src/main/resources/KafkaToGcsIT/avro_schema_1.avsc";
    Schema avroSchema1 = new Schema.Parser().parse(new File(schemaPath1));

    String schemaPath2 = "/Users/anandinguva/Desktop/projects/DataflowTemplates/v2/kafka-to-gcs/src/main/resources/KafkaToGcsIT/avro_schema_2.avsc";
    Schema avroSchema2 = new Schema.Parser().parse(new File(schemaPath2));


    schemaRegistryClient.register("subject", avroSchema1, 1, 1);
    schemaRegistryClient.register("subject", avroSchema2, 1, 2);

    KafkaProducer<String, GenericRecord> kafkaProducer = createStringProducer(bootStrapServer);

    GenericRecord genericRecord1 = new GenericRecordBuilder(avroSchema1)
            .set("productId", 1)
            .set("productName", "test")
            .build();

//    GenericRecord genericRecord2 = new GenericRecordBuilder(avroSchema2)
//            .set("productId", 1)
//            .set("productName", "test2")
//            .set("productDescription", "Testing schema")
//            .build();

    GenericRecord genericRecord2 = new GenericRecordBuilder(avroSchema2)
            .set("name", "anand")
            .set("age", 10)
            .set("email", "anandinguva98@gmail.com")
            .build();

    RecordMetadata recordMetadata1 = kafkaProducer.send(new ProducerRecord<>(topic, "hello", genericRecord1)).get();
    RecordMetadata recordMetadata2 = kafkaProducer.send(new ProducerRecord<>(topic, "hello", genericRecord2)).get();

  }


  /** Tests testFileFormat() with a valid File format. */
  @Test
  public void testFileFormatValid() {
    String fileFormat = "avro".toUpperCase();

    Boolean status = WriteToGCSUtility.isValidFileFormat(fileFormat);
    assertThat(status, is(notNullValue()));
    assertThat(status, is(equalTo(true)));
  }
}
