package com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.impl;

import com.code.fuqinqin.kafkalearn.common.util.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * 生产线程
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
public abstract class AbstractProduceThread implements Runnable {
    protected KafkaProducer<String, String> kafkaProducer;
    protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public AbstractProduceThread(String brokerList) {
        Properties properties = Configuration.buildProducerConfig(brokerList);
        kafkaProducer = new KafkaProducer<>(properties);
    }
}
