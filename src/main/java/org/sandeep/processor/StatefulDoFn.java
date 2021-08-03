package org.sandeep.processor;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;

public class StatefulDoFn extends DoFn<KV<String, String>, KV<String, String>> {

    @StateId("word_counts")
    private final StateSpec<ValueState<String>> word_counts = StateSpecs.value(StringUtf8Coder.of());

    public StatefulDoFn() {

    }

    @ProcessElement
    public void process(
            ProcessContext context,
            @StateId("word_counts") ValueState<String> wordCounts
    ) {
        String rawEvent = context.element().getValue();
        System.out.println("Received input: "+rawEvent);
        String ividCount = wordCounts.read();
        System.out.println("Current count: "+ividCount);
        if(StringUtils.isEmpty(ividCount))
            wordCounts.write(String.valueOf(1));
        else
            wordCounts.write(String.valueOf(Integer.parseInt(ividCount) + 1));
        context.output(KV.of(rawEvent, rawEvent));
    }
}