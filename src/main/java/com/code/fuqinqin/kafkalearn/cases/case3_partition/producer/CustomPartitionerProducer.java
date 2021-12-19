package com.code.fuqinqin.kafkalearn.cases.case3_partition.producer;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 自定义分区器的消息生产者
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Slf4j
@Component
public class CustomPartitionerProducer {
    @Value("${kafka.broker.list}")
    private String brokerList;

    @PostConstruct
    public void sendMsg() throws ExecutionException, InterruptedException {
        Properties properties = Configuration.buildProducerConfig(brokerList, DemoPartitioner.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO, 3, "11", "value-1");
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
        log.info("produce value = {}, partition = {}, topic = {}, offset = {}, timestamp = {}",
                record.value(), recordMetadata.partition(), recordMetadata.topic(),
                recordMetadata.offset(), recordMetadata.timestamp());
        kafkaProducer.close();
    }
}
