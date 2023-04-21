package com.wdyin.kafka.delay;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka同步消费者
 * @author WDYin
 * @date 2023/4/14
 **/
public class KafkaSyncConsumer<K, V> extends KafkaConsumer<K, V> {

    KafkaSyncConsumer(Properties properties) {
        super(properties);
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        return super.poll(timeout);
    }

    @Override
    public synchronized Set<TopicPartition> paused() {
        return super.paused();
    }

    synchronized void pauseAndSeek(TopicPartition partition, long offset) {
        super.pause(Collections.singletonList(partition));
        super.seek(partition, offset);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        super.commitSync(offsets);
    }

    synchronized void resume(TopicPartition topicPartition) {
        super.resume(Collections.singleton(topicPartition));
    }

    @Override
    public synchronized  void commitSync(){
        super.commitSync();
    }
}
