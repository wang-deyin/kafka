package com.wdyin.kafka.config;

import com.wdyin.kafka.delay.KafkaDelayConfig;
import com.wdyin.kafka.delay.KafkaDelayQueueFactory;
import com.wdyin.kafka.delay.KafkaPollListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author WDYin
 * @date 2023/4/21
 **/
@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Resource
    private ApplicationContext applicationContext;

    /**
     * 消费者参数配置
     * @param bootstrapServers
     * @param isAutoSubmit
     * @return
     */
    private Map<String, Object> consumerProps(String bootstrapServers, Boolean isAutoSubmit) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoSubmit);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * spring生产者参数配置
     * @param bootstrapServer
     * @return
     */
    private HashMap<String, Object> producerProps(String bootstrapServer) {
        HashMap<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        return configProps;
    }

    /**
     * spring kafkaTemplate注册
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps(bootstrapServers)));
    }

    /**
     * 延时队列-Kafka同步消费者配置
     * @return
     */
    public Properties kafkaSyncConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "15000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    /**
     * 延时队列-注册延时队列工厂
     * @return
     */
    @Bean
    public KafkaDelayQueueFactory kafkaDelayQueueFactory() {
        KafkaDelayConfig kafkaDelayConfig = new KafkaDelayConfig();
        kafkaDelayConfig.setPollThreadPool(1);
        kafkaDelayConfig.setPollTimeout(50);
        kafkaDelayConfig.setPollInterval(50);
        kafkaDelayConfig.setDelayThreadPool(10);
        KafkaDelayQueueFactory kafkaDelayQueueFactory = new KafkaDelayQueueFactory(kafkaSyncConsumerProperties(), kafkaDelayConfig);
        kafkaDelayQueueFactory.setApplicationContext(applicationContext);
        return kafkaDelayQueueFactory;
    }

    /**
     * 延时队列-注册消费者poll监听器
     * @param kafkaTemplate
     * @return
     */
    @Bean
    public KafkaPollListener kafkaPollListener(KafkaTemplate kafkaTemplate) {
        return new KafkaPollListener<>(kafkaTemplate);
    }

}
