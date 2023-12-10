package com.h12.beam.streaming;

import com.google.common.collect.ImmutableMap;
import com.h12.beam.streaming.stages.KafkaRecordTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipelineMain {
    public static void main(String[] args) {
        // Set up the pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Define Kafka configuration
        String bootstrapServers = "your-kafka-bootstrap-servers";
        String topic = "your-kafka-topic";
        KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopics(Arrays.asList(topic))
                .updateConsumerProperties(ImmutableMap.of("group.id", "my-group-id"))
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .commitOffsetsInFinalize();

        // KafkaIO.Read to consume messages from Kafka
        pipeline.apply("ReadFromKafka", read)
                // Apply business logic in multiple stages
                .apply("RecordsTranslator", ParDo.of(new DoFn<KafkaRecord<String,String>, KafkaRecord<String,String>>() {
                    private final Counter counter = Metrics.counter(DoFn.class, "processed");
                    private final Gauge gauge = Metrics.gauge(DoFn.class, "my_gauge");

                    @DoFn.ProcessElement
                    public void processElement(ProcessContext c) {
                        counter.inc();

                        KafkaRecord<String, String> record = c.element();
                        KV<String, String> log = record.getKV();
                        System.out.println("Key Obtained: " + log.getKey());
                        System.out.println("Value Obtained: " + log.getValue().toString());
                        // Save to DB.
                        c.output(record);
                    }
                }))
                .apply("KafkaMapper", ParDo.of(new DoFn<KafkaRecord<String, String>, Map<String, String>>() {
                    @DoFn.ProcessElement
                    public void processElement(ProcessContext c) {
                        KafkaRecord<String, String> record = c.element();
                        Map<String, String> map = new HashMap<>();
                        KV<String, String> log = record.getKV();
                        c.output(map);
                    }
                }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();

    }

    // Example DoFn for the first stage of processing
    static class FirstStage extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            // Apply your first stage business logic here
            // For example, convert the message to uppercase
            return input.getValue().toUpperCase();
        }
    }

    // Example DoFn for the second stage of processing
    static class SecondStage extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            // Apply your second stage business logic here
            // For example, add a suffix to the message
            return input + "_processed";
        }
    }
}