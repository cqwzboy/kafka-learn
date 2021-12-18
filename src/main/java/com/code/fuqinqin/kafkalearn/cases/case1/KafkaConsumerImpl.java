package com.code.fuqinqin.kafkalearn.cases.case1;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 消息消费端
 *
 * @author fuqinqin
 * @date 2021-12-18
 * */
@Slf4j
@Component
public class KafkaConsumerImpl {
    @Value("${kafka.broker.list}")
    private String brokerList;
    @Value("${kafka.group-id}")
    private String groupId;

    @PostConstruct
    public void consumer(){
        new Thread(new ConsumerThread()).start();
        log.info("消费端已经启动...");
    }

    private class ConsumerThread implements Runnable{
        @Override
        public void run() {
            Properties properties = Configuration.buildConsumerConfig(brokerList, groupId);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(TopicConstant.TOPIC_DEMO));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("record.value() = {}", record.value());
                }
            }
        }
    }
}
