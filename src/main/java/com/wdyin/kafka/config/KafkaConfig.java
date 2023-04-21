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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

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
        //kafka broker地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //取消自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoSubmit);
        //一次拉取消息数量,可根据实际情况自行调整
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        //序列化
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
        //broke地址
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //序列化
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //幂等发送给broker
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //生产者时将多少数据累积到一个批次中，设置为0的目的提高实时性
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        return configProps;
    }

    /**
     * 用于spring的@listener注解进行消费，并非用于延时队列
     * @return
     */
    @Bean("kafkaContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps(bootstrapServers, Boolean.FALSE)));
        //线程数为1
        factory.setConcurrency(1);
        //poll超时时间
        factory.getContainerProperties().setPollTimeout(1500L);
        //手动立即提交offset
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
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
        // Consumer的配置
        Properties properties = new Properties();
        // 服务地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // 关闭offset自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 消费者offset自动提交到Kafka的频率（以毫秒为单位）
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "15000");

        // KEY的反序列化器类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // VALUE的反序列化器类
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
