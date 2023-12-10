package com.h12.beam.streaming.stages;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Map;

public class KafkaRecordTransform extends PTransform<PCollection<KafkaRecord<String, String>>, POutput> {

    @Override
    public POutput expand(PCollection<KafkaRecord<String, String>> input) {
        for (Map.Entry<TupleTag<?>, PValue> iter: input.expand().entrySet()) {

        }
        return null;
    }
}
