package com.wdyin.kafka.delay;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * 延时队列工厂
 * @author WDYin
 * @date 2023/4/17
 **/
@Data
public class KafkaDelayQueueFactory {

    private KafkaDelayConfig kafkaDelayConfig;
    private Properties properties;
    private ApplicationContext applicationContext;
    private Integer concurrency;

    public KafkaDelayQueueFactory(Properties properties, KafkaDelayConfig kafkaDelayConfig) {
        Assert.notNull(properties, "properties cannot null");
        Assert.notNull(kafkaDelayConfig.getDelayThreadPool(), "delayThreadPool cannot null");
        Assert.notNull(kafkaDelayConfig.getPollThreadPool(), "pollThreadPool cannot null");
        Assert.notNull(kafkaDelayConfig.getPollInterval(), "pollInterval cannot null");
        Assert.notNull(kafkaDelayConfig.getPollTimeout(), "timeout cannot null");
        this.properties = properties;
        this.kafkaDelayConfig = kafkaDelayConfig;
    }

    /**
     * Kafka将消息从【延时主题】经过【延时时间】后发送到【目标主题】
     * @param topic 延时主题
     * @param group 消费者组
     * @param delayTime 延时时间
     * @param targetTopic 目标主题
     */
    public void listener(String topic, String group, Integer delayTime, String targetTopic) {
        if (StringUtils.isEmpty(topic)) {
            throw new RuntimeException("topic cannot empty");
        }
        if (StringUtils.isEmpty(group)) {
            throw new RuntimeException("group cannot empty");
        }
        if (StringUtils.isEmpty(delayTime)) {
            throw new RuntimeException("delayTime cannot empty");
        }
        if (StringUtils.isEmpty(targetTopic)) {
            throw new RuntimeException("targetTopic cannot empty");
        }
        KafkaSyncConsumer<String, String> kafkaSyncConsumer = createKafkaSyncConsumer(group);
        KafkaDelayQueue<String, String> kafkaDelayQueue = createKafkaDelayQueue(topic, group, delayTime, targetTopic, kafkaSyncConsumer);
        kafkaDelayQueue.send();
    }

    private KafkaDelayQueue<String, String> createKafkaDelayQueue(String topic, String group, Integer delayTime, String targetTopic, KafkaSyncConsumer<String, String> kafkaSyncConsumer) {
        KafkaDelayQueue<String, String> kafkaDelayQueue = new KafkaDelayQueue<>(kafkaSyncConsumer, kafkaDelayConfig);
        Assert.notNull(applicationContext, "kafkaDelayQueue need applicationContext");
        kafkaDelayQueue.setApplicationContext(applicationContext);
        kafkaDelayQueue.setDelayTime(delayTime);
        kafkaDelayQueue.setTopic(topic);
        kafkaDelayQueue.setGroup(group);
        kafkaDelayQueue.setTargetTopic(targetTopic);
        return kafkaDelayQueue;
    }

    private KafkaSyncConsumer<String, String> createKafkaSyncConsumer(String group) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return new KafkaSyncConsumer<>(properties);
    }

}
