package com.galibots.dailymeeting;

import com.eclipsesource.json.JsonObject;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DailyKafkaConsumer implements Runnable {
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;
    private DailyMeetingMessageHandler dailyHandler;

    public DailyKafkaConsumer(int id, String groupId, List<String> topics, DailyMeetingMessageHandler dailyHandler) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        this.dailyHandler = dailyHandler;
    }

    @Override
    public void run() {
        try {

            consumer.subscribe(topics);

            while (dailyHandler.keepRunning.get()) {


                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());

                    System.out.println(this.id + ": " + data.get("value"));

                    JsonObject message = JsonObject.readFrom(record.value());

                    if (message.names().contains("type") &&
                            message.get("type").asString().equals("message") &&
                            message.get("text").asString().equals("hello 2")) {

                        String slackChannel = message.get("channel").asString();

                        message = new JsonObject()
                                .add("id", 1)
                                .add("type", "message")
                                .add("channel", slackChannel)
                                .add("text", message.get("text").asString());

                        boolean channelFound = false;
                        for (Channel channel : dailyHandler.channels) {
                            channelFound = true;
                            channel.writeAndFlush(new TextWebSocketFrame(message.toString()));
                        }

                        if (!channelFound) {
                            System.out.println("Can't get channel because id wasn't set!");
                        }
                    }
                }

            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
