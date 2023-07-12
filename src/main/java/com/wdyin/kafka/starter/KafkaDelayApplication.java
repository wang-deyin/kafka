package com.wdyin.kafka.starter;

import com.wdyin.kafka.delay.KafkaDelayQueueFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Kafka延时队列启动程序
 * @author WDYin
 * @date 2023/4/18
 **/
@Component
public class KafkaDelayApplication {

    @Resource
    private KafkaDelayQueueFactory kafkaDelayQueueFactory;

    /**
     * 延迟任务都可以配置在这里
     * Kafka将消息从【延时主题】经过【延时时间】后发送到【目标主题】
     */
    @PostConstruct
    public void init() {
        //延迟30秒
        kafkaDelayQueueFactory.listener("delay-30-second-topic", "delay-30-second-group", 30 * 1000, "delay-60-second-target-topic");
        //延迟60秒
        kafkaDelayQueueFactory.listener("delay-60-second-topic", "delay-60-second-group", 60 * 1000, "delay-60-second-target-topic");
        //延迟30分钟
        kafkaDelayQueueFactory.listener("delay-30-minute-topic", "delay-30-minute-group", 30 * 60 * 1000, "delay-30-minute-target-topic");
    }
}
