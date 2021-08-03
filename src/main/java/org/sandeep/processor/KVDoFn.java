package org.sandeep.processor;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class KVDoFn {
    public static MapElements<KafkaRecord<String, String>, KV<String, String>> makeKeyValue() {
        return MapElements.via(
                new SimpleFunction<KafkaRecord<String, String>, KV<String, String>>() {
                    @Override
                    public KV<String, String> apply(KafkaRecord<String, String> input) {
                        String record = input.getKV().getValue();
                        return KV.of(record, record);
                    }
                }
        );
    }
}

/*public class KVDoFn extends DoFn<KafkaRecord<String, String>, KV<String, String>> {

    @ProcessElement
    public void format(ProcessContext context, @Element KafkaRecord<String, String> input) {
        String rec = input.getKV().getValue();
        context.output(KV.of(rec, rec));
    }
}*/