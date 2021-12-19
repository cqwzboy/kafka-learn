package com.code.fuqinqin.kafkalearn.cases.case1_start;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 消息生产端
 *
 * @author fuqinqin
 * @date 2021-12-18
 * */
@Slf4j
@Component
public class KafkaProducerImpl {
    @Value("${kafka.broker.list}")
    private String brokerList;

    @PostConstruct
    public void sendMsg() throws InterruptedException, ExecutionException {
        Properties properties = Configuration.buildProducerConfig(brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        while (true){
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
            RecordMetadata recordMetadata = producer.send(record).get();
            log.info("produce value = {}, partition = {}, topic = {}, offset = {}, timestamp = {}",
                    record.value(), recordMetadata.partition(), recordMetadata.topic(),
                    recordMetadata.offset(), recordMetadata.timestamp());
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
