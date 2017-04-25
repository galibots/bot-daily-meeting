package com.galibots.dailymeeting;

import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class DailyMeetingLauncher {

    // TODO: move to an env. variable
    final String bootstrapServer = "localhost:9092";

    private static final ChannelGroup channels = new DefaultChannelGroup("galibotChannelGroup", GlobalEventExecutor.INSTANCE);

    public static void main(String[] args) throws Exception {

        DailyMeetingMessageHandler messageHandler = new DailyMeetingMessageHandler(channels);

        int numConsumers = 3;
        String groupId = "daily-group";
        List<String> topics = Arrays.asList("example");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<DailyKafkaConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            DailyKafkaConsumer consumer = new DailyKafkaConsumer(i, groupId, topics, messageHandler);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (DailyKafkaConsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }
}
