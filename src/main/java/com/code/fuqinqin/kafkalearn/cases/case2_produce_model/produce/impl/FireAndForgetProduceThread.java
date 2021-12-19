package com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.impl;

import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceCounter;
import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceType;
import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;

/**
 * 发后即忘生产线程
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
public class FireAndForgetProduceThread extends AbstractProduceThread {
    public FireAndForgetProduceThread(String brokerList) {
        super(brokerList);
    }

    @Override
    public void run() {
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO,
                    "发后即忘" + super.sdf.format(new Date()));
            super.kafkaProducer.send(record);
            ProduceCounter.addAndGet(1, ProduceType.FIRE_AND_FORGET);
        }
    }
}
