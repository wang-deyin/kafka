package com.wdyin.kafka.delay;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * 延时队列监听
 * @Author : WDYin
 * @Date : 2021/5/7
 * @Desc :
 */
@Slf4j
public class KafkaPollListener<K, V> implements ApplicationListener<KafkaPollEvent<K, V>> {

    private KafkaTemplate kafkaTemplate;

    public KafkaPollListener(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void onApplicationEvent(KafkaPollEvent<K, V> event) {
        ConsumerRecords<K, V> records = (ConsumerRecords<K, V>) event.getSource();
        Integer delayTime = event.getDelayTime();
        KafkaDelayQueue<K, V> kafkaDelayQueue = event.getKafkaDelayQueue();
        KafkaSyncConsumer<K, V> kafkaSyncConsumer = kafkaDelayQueue.getKafkaSyncConsumer();

        //1.获取poll到的有消息的分区
        Set<TopicPartition> partitions = records.partitions();

        //2.存储需要commit的消息,提高效率批量提交
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        //3.遍历有消息的分区
        partitions.forEach((partition) -> {
            List<ConsumerRecord<K, V>> consumerRecords = records.records(partition);
            //4.遍历分区里面的消息
            for (ConsumerRecord<K, V> record : consumerRecords) {
                //5.获取消息创建时间
                long startTime = (record.timestamp() / 1000) * 1000;
                long endTime = startTime + delayTime;
                //6.不符合条件的分区暂停消费
                long now = System.currentTimeMillis();
                if (endTime > now) {
                    kafkaSyncConsumer.pauseAndSeek(partition, record.offset());
                    //7.使用 schedule()执行定时任务
                    kafkaDelayQueue.getThreadPoolPollTaskScheduler().schedule(kafkaDelayQueue.delayTask(partition), new Date(endTime));
                    //无需继续消费该分区下的其他消息，直接消费其他分区
                    break;
                }
                log.info("{}: partition:{}, offset:{}, key:{}, value:{}, messageDate:{}, nowDate:{}, messageDate:{}, nowDate:{}",
                        Thread.currentThread().getName() + "#" + Thread.currentThread().getId(), record.topic() + "-" + record.partition(), record.offset(), record.key(), record.value(), LocalDateTime.ofInstant(Instant.ofEpochMilli(startTime), ZoneId.systemDefault()), LocalDateTime.now(), startTime, Instant.now().getEpochSecond());
                //发送目标主题
                kafkaTemplate.send(kafkaDelayQueue.getTargetTopic(), record.value());
                //更新需要commit的消息
                commitMap.put(partition, new OffsetAndMetadata(record.offset() + 1));
            }
        });
        //8.批量提交,提高效率,commitSync耗时几百毫秒
        if (!commitMap.isEmpty()) {
            kafkaSyncConsumer.commitSync(commitMap);
        }
    }
}
