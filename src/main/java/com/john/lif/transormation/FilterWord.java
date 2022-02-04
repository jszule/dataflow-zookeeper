package com.john.lif.transormation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.RetryNTimes;

import java.util.concurrent.ConcurrentHashMap;


public class FilterWord extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new FilterDoFn()));
    }

    private static class FilterDoFn extends DoFn<String, String> {

        ConcurrentHashMap<String, String> myMap = new ConcurrentHashMap<>();

        @Setup
        public void setup() throws Exception {
            CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new RetryNTimes(5, 5));
            client.start();
            PersistentWatcher persistentWatcher = new PersistentWatcher(client, "/aa/bb", false);
            persistentWatcher.start();

            myMap.put("a",  new String(client.getData().forPath("/aa/bb")));

            persistentWatcher.getListenable().addListener(event -> {
                        try {
                            var result = new String(client.getData().forPath(event.getPath()));
                            System.out.println("In listener:" + result + " " + Thread.currentThread().getName());
                            myMap.put("a", result);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
            );
        }

        @Teardown
        public void tearDown() {
            //close client connections
        }

        @ProcessElement
        public void processElement(@Element String inputString, OutputReceiver<String> outputReceiver) {
            System.out.println("Config: " + myMap.get("a"));
            outputReceiver.output(inputString);
        }
    }
}
