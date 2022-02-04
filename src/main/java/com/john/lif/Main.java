package com.john.lif;

import com.john.lif.transormation.FilterWord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.util.Arrays;

import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;

public class Main {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setRunner(DirectRunner.class);
        dataflowOptions.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read()
                        .from("/home/john/test-dir/*")
                        .watchForNewFiles(Duration.millis(500), afterTimeSinceNewOutput(Duration.standardHours(1))))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()))
                .apply(new FilterWord())
                .apply(Count.perElement())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(
                                        (KV<String, Long> wordCount) ->
                                                wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("wordout/wordcounts").withWindowedWrites().withNumShards(1));

        p.run();
    }
}

