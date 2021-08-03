package org.sandeep.processor;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Processor {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "once-group-id");
        String inputTopicName = "inp-topic";
        String outputTopicName = "oup-topic";


        HashMap<String, Object> kafkaProperties = new HashMap<>();
        for (final String name: kafkaProps.stringPropertyNames()) {
            kafkaProperties.put(name, kafkaProps.getProperty(name));
        }

        String[] arr = new String[6];
        arr[0] = "--runner=FlinkRunner";
        arr[1] = "--parallelism=1";
        arr[2] = "--flinkMaster=[auto]";
        arr[3] = "--numberOfExecutionRetries=0";
        arr[4] = "--experiments=use_deprecated_read";
        arr[5] = "--fasterCopy=True";


        PipelineOptions options = PipelineOptionsFactory.fromArgs(arr).withValidation().create();
        Pipeline p = Pipeline.create(options);

        p.apply("read from kafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                        .withTopic(inputTopicName)
                        .withKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class)
                        .withValueDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class)
                        .updateConsumerProperties(kafkaProperties)
                        .commitOffsetsInFinalize()
                        .withKeyDeserializerAndCoder(org.apache.kafka.common.serialization.StringDeserializer.class, StringUtf8Coder.of())
                        .withValueDeserializerAndCoder(org.apache.kafka.common.serialization.StringDeserializer.class, StringUtf8Coder.of()

                        )
        )
        .apply("Convert to Key value pairs", KVDoFn.makeKeyValue()).setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("Word count", ParDo.of(new StatefulDoFn()))
        .apply("write to kafka", KafkaIO.<String, String>write()
             .withBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                        .withTopic(outputTopicName)
                        .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
                        .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class)
                        .updateProducerProperties(kafkaProperties)
        );

        p.run().waitUntilFinish();
    }
}
