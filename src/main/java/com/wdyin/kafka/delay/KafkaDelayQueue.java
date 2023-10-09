package com.wdyin.kafka.delay;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * kafka延时队列
 *
 * @Author WDYin
 * @Date 2022/7/2
 **/
@Slf4j
@Getter
@Setter
class KafkaDelayQueue<K, V> {

    private String topic;
    private String group;
    private Long delayTime;
    private String targetTopic;
    private KafkaDelayConfig kafkaDelayConfig;
    private KafkaSyncConsumer<K, V> kafkaSyncConsumer;
    private ApplicationContext applicationContext;
    private ThreadPoolTaskScheduler threadPoolPollTaskScheduler;
    private ThreadPoolTaskScheduler threadPoolDelayTaskScheduler;

    KafkaDelayQueue(KafkaSyncConsumer<K, V> kafkaSyncConsumer, KafkaDelayConfig kafkaDelayConfig) {
        this.kafkaSyncConsumer = kafkaSyncConsumer;
        this.kafkaDelayConfig = kafkaDelayConfig;
        createThreadPoolPollTaskScheduler();
        createThreadPoolDelayTaskScheduler();
    }

    void send() {
        try {
            kafkaSyncConsumer.subscribe(Collections.singletonList(topic));
            this.threadPoolPollTaskScheduler
                    .scheduleWithFixedDelay(pollTask(), kafkaDelayConfig.getPollInterval());
        } catch (Exception e) {
            log.error("KafkaDelayQueue subscribe error", e);
        }
    }

    private void createThreadPoolDelayTaskScheduler() {
        threadPoolDelayTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolDelayTaskScheduler.initialize();
        threadPoolDelayTaskScheduler.setPoolSize(kafkaDelayConfig.getPollThreadPool());
        threadPoolDelayTaskScheduler.setThreadNamePrefix("kafkaDelayTaskScheduler-");
        threadPoolDelayTaskScheduler.setAwaitTerminationSeconds(60);
        threadPoolDelayTaskScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolDelayTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
    }

    private void createThreadPoolPollTaskScheduler() {
        threadPoolPollTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolPollTaskScheduler.initialize();
        threadPoolPollTaskScheduler.setPoolSize(kafkaDelayConfig.getDelayThreadPool());
        threadPoolPollTaskScheduler.setThreadNamePrefix("KafkaPollTaskScheduler-");
        threadPoolPollTaskScheduler.setAwaitTerminationSeconds(60);
        threadPoolPollTaskScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolPollTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
    }

    private KafkaPollTask<K, V> pollTask(){
        return new KafkaPollTask<>(this, Duration.ofMillis(kafkaDelayConfig.getPollTimeout()), delayTime, applicationContext);
    }

    KafkaDelayTask<K, V> delayTask(TopicPartition partition){
        return new KafkaDelayTask<>(kafkaSyncConsumer, partition);
    }

    @Slf4j
    private static class KafkaPollTask<K, V> implements Runnable {

        private KafkaDelayQueue<K, V> kafkaDelayQueue;
        private Duration timeout;
        private Long delayTime;
        private ApplicationContext applicationContext;

        KafkaPollTask(KafkaDelayQueue<K, V> kafkaDelayQueue, Duration timeout, Long delayTime, ApplicationContext applicationContext) {
            this.kafkaDelayQueue = kafkaDelayQueue;
            this.timeout = timeout;
            this.applicationContext = applicationContext;
            this.delayTime = delayTime;
        }

        @Override
        public void run() {
            try {
                ConsumerRecords<K, V> records = kafkaDelayQueue.getKafkaSyncConsumer().poll(timeout);
                applicationContext.publishEvent(new KafkaPollEvent<>(records, delayTime, kafkaDelayQueue));
            } catch (Exception e) {
                log.error("KafkaDelayQueue consumer fail", e);
            }
        }
    }

    @Slf4j
    private static class KafkaDelayTask<K, V> implements Runnable {
        private KafkaSyncConsumer<K, V> kafkaSyncConsumer;
        private TopicPartition partition;

        private KafkaDelayTask(KafkaSyncConsumer<K, V> kafkaSyncConsumer, TopicPartition partition) {
            this.kafkaSyncConsumer = kafkaSyncConsumer;
            this.partition = partition;
        }

        @Override
        public void run() {
            try {
                kafkaSyncConsumer.resume(partition);
            } catch (Exception e) {
                log.error("KafkaDelayQueue resume failed", e);
            }
        }
    }
}
