package com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.impl;

import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceCounter;
import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceType;
import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;

/**
 * 同步生产线程
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
public class SyncProduceThread extends AbstractProduceThread {
    public SyncProduceThread(String brokerList) {
        super(brokerList);
    }

    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO,
                    "同步-" + super.sdf.format(new Date()));
            super.kafkaProducer.send(record).get();
            ProduceCounter.addAndGet(1, ProduceType.SYNC);
        }
    }
}
