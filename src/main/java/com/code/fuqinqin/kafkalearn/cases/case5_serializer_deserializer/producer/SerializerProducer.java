package com.code.fuqinqin.kafkalearn.cases.case5_serializer_deserializer.producer;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 拦截器测试生产者
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Slf4j
@Component
public class SerializerProducer {
    @Value("${kafka.broker.list}")
    private String brokerList;

    @PostConstruct
    public void sendMsg() throws ExecutionException, InterruptedException {
        Properties properties = Configuration.buildProducerConfig(brokerList, null, null, StudentSerializer.class);
        KafkaProducer<String, StudentDTO> kafkaProducer = new KafkaProducer<>(properties);
        StudentDTO studentDTO = new StudentDTO();
        studentDTO.setNo(1L);
        studentDTO.setName("张三");
        studentDTO.setBirthday(new Date());
        ProducerRecord<String, StudentDTO> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO, studentDTO);
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
        log.info("produce value = {}, partition = {}, topic = {}, offset = {}, timestamp = {}",
                record.value(), recordMetadata.partition(), recordMetadata.topic(),
                recordMetadata.offset(), recordMetadata.timestamp());
        kafkaProducer.close();
    }
}
