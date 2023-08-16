package com.wdyin.kafka.delay;

import org.springframework.context.ApplicationEvent;

/**
 * 延时队列事件
 * @Author : WDYin
 * @Date : 2021/5/7
 * @Desc :
 */
class KafkaPollEvent<K, V> extends ApplicationEvent {
    private Long delayTime;
    private KafkaDelayQueue<K, V> kafkaDelayQueue;

    KafkaPollEvent(Object source, Long delayTime, KafkaDelayQueue<K, V> kafkaDelayQueue) {
        super(source);
        this.delayTime = delayTime;
        this.kafkaDelayQueue = kafkaDelayQueue;
    }

    Long getDelayTime() {
        return delayTime;
    }

    public KafkaDelayQueue<K, V> getKafkaDelayQueue() {
        return kafkaDelayQueue;
    }

}
