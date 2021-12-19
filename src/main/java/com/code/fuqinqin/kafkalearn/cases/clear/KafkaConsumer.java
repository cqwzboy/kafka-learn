package com.code.fuqinqin.kafkalearn.cases.clear;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息消费端
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
@Slf4j
@Component
public class KafkaConsumer {
    @Value("${kafka.broker.list}")
    private String brokerList;
    @Value("${kafka.group-id}")
    private String groupId;

    private AtomicLong consumerCounter = new AtomicLong(0);

    @PostConstruct
    public void consumer() {
        new Thread(new ConsumerThread()).start();
        log.info("消费端已经启动...");
        new Timer().schedule(new TimerTask() {
            private int statisticsNum = 0;

            @Override
            public void run() {
                log.info("第{}次统计，已消费{}条消息", ++statisticsNum, consumerCounter.get());
            }
        }, 5000, 5000);
    }

    private class ConsumerThread implements Runnable {
        @Override
        public void run() {
            Properties properties = Configuration.buildConsumerConfig(brokerList, groupId);
            org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(TopicConstant.TOPIC_DEMO));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                consumerCounter.addAndGet(records.count());
            }
        }
    }
}
