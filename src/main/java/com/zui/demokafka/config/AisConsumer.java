package com.zui.demokafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * 消费kafka数据
 * topic 为 xx_capture_img_5_db
 * @author aissue
 */
@Component
@Slf4j
@EnableScheduling
public class AisConsumer {
    //kafkaListener 唯一id
    private static final String TOPIC_ID= "AisListener";
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    /**
     * 消费指定topic下的
     * @param
     */
    @KafkaListener(id = TOPIC_ID, topics = "${kafka.consumer.topic}", groupId = "${kafka.consumer.group-id}",containerFactory = "kafkaListenerContainerFactory")
    public void getImg5DbData(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("kafka message:{}");
        if (records == null || records.size() == 0) {
            return;
        }
        for (ConsumerRecord<String, String> record : records) {
            Optional<String> kafkaMessage = Optional.ofNullable(record.value());
            if (kafkaMessage.isPresent()) {
                String msg = kafkaMessage.get();
                log.info("kafka message:{}",msg);
            }
        }
        ack.acknowledge();
    }

    /**
     * 容器启动3000ms后执行
     * 每12小时执行一次

    //@Scheduled(initialDelay=3000, fixedDelay=12*60*60*1000)
    public void start(){
        log.info("kafka listener enabled【{}】",enableListen);
        MessageListenerContainer container = registry.getListenerContainer(ZHYY);
        if (!container.isRunning()) {
            container.start();
        }
        //恢复
        container.resume();
    }
     **/

}
