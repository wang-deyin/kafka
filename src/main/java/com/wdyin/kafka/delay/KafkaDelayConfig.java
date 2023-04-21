package com.wdyin.kafka.delay;

import lombok.Data;

/**
 * 延时队列配置
 * @author WDYin
 * @date 2023/4/16
 **/
@Data
public class KafkaDelayConfig {

    /**
     * 消费者poll之间的间隔：毫秒
     */
    private Integer pollInterval;
    /**
     * 消费者poll超时时间：毫秒
     */
    private Integer pollTimeout;
    /**
     * 消费者poll的线程池
     */
    private Integer pollThreadPool;
    /**
     * 消费者延时任务的线程池
     */
    private Integer delayThreadPool;

    public KafkaDelayConfig() {
    }
}
