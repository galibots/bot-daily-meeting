package com.galibots.dailymeeting;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class DailyMeetingMessageHandler implements DailyMeetingInterface {

    // TODO: make this private
    protected final ConcurrentLinkedQueue<String> requestQueue = new ConcurrentLinkedQueue<>();

    // TODO: make this private
    protected final ChannelGroup channels;

    // TODO: make this private
    protected static final AtomicBoolean keepRunning = new AtomicBoolean(true);

    public DailyMeetingMessageHandler(ChannelGroup channels) {
        this.channels = channels;

        KafkaProducerCallable toKafkaCallable = new KafkaProducerCallable(this);
        FutureTask<String> toKafka = new FutureTask<>(toKafkaCallable);

        ExecutorService toKafkaExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("to-kafka-%d")
                        .build()
        );
        toKafkaExecutor.execute(toKafka);
    }

    public String handleMessage(ChannelHandlerContext ctx, String frameText) {

        // TODO: here we could parse the JSON and filter by type of messages
        requestQueue.add(frameText);
        return null;
    }

}
