package com.wdyin.kafka.delay;

import lombok.Data;

/**
 * 延时队列配置
 * @author WDYin
 * @date 2023/4/16
 **/
@Data
public class KafkaDelayConfig {

    private Integer pollInterval;
    private Integer pollTimeout;
    private Integer pollThreadPool;
    private Integer delayThreadPool;

    public KafkaDelayConfig() {
    }
}
