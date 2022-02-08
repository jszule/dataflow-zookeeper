package com.john.lif;

import com.john.lif.transormation.FilterWord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

public class Main {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setRunner(DirectRunner.class);
        dataflowOptions.setStreaming(true);
        dataflowOptions.setPubsubRootUrl("http://localhost:8085");

        Pipeline p = Pipeline.create(options);

        p.apply(PubsubIO.readStrings().fromTopic("projects/lif-john-test/topics/test"))
                .apply(new FilterWord())
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String string) {
                        System.out.println(string);
                    }
                }));

        p.run();
    }
}

