package com.zui.demokafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@PropertySource("classpath:kafka.properties")
public class KafkaConsumerConfig {
    @Value("${kafka.consumer.servers}")
    private String servers;
    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;
    @Value("${kafka.consumer.session.timeout}")
    private int sessionTimeout;
    @Value("${kafka.consumer.request.timeout}")
    private int requestTimeout;
    @Value("${kafka.consumer.auto.commit.interval}")
    private String autoCommitInterval;
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;
    @Value("${kafka.consumer.concurrency}")
    private int concurrency;
    @Value("${kafka.consumer.max.poll.records}")
    private int maxPollRecords;
    @Value("${kafka.consumer.poll.timeout}")
    private int pollTimeout;
    @Value("${kafka.consumer.max.partition.fetch.bytes}")
    private int maxPartitionFetchBytes;
    @Value("${kafka.consumer.heartbeat.interval.ms}")
    private int heartbeatInterval;
    @Value("${kafka.consumer.max.poll.interval.ms}")
    private int maxPollInterval;
    @Value("${kafka.consumer.autoStart}")
    private boolean autoStart;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Long>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Long> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true);
        factory.getContainerProperties().setPollTimeout(pollTimeout);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setAutoStartup(autoStart);
        return factory;
    }

    public ConsumerFactory<String, Long> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);

        //字符串消费
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);//每一批数量
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        propsMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        //添加以下两个配置，解决consumer消费停止的问题（心跳过期，退出消费者群组问题）
        propsMap.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        propsMap.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        return propsMap;
    }
}