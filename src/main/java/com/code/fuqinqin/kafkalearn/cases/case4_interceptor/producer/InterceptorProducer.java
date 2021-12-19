package com.code.fuqinqin.kafkalearn.cases.case4_interceptor.producer;

import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
public class InterceptorProducer {
    @Value("${kafka.broker.list}")
    private String brokerList;

    @PostConstruct
    public void sendMsg() throws ExecutionException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // 定义多个拦截器时，执行顺序与设置时的顺序一致
        Properties properties = Configuration.buildProducerConfig(brokerList, null, Arrays.asList(ProducePrefix1Interceptor.class, ProducePrefix2Interceptor.class), null);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO, sdf.format(new Date()));
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            log.info("produce value = {}, partition = {}, topic = {}, offset = {}, timestamp = {}",
                    record.value(), recordMetadata.partition(), recordMetadata.topic(),
                    recordMetadata.offset(), recordMetadata.timestamp());
        }
        kafkaProducer.close();
    }
}
