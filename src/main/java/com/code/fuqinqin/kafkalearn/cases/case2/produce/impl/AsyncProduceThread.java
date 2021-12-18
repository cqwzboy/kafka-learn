package com.code.fuqinqin.kafkalearn.cases.case2.produce.impl;

import com.code.fuqinqin.kafkalearn.cases.case2.produce.ProduceCounter;
import com.code.fuqinqin.kafkalearn.cases.case2.produce.ProduceType;
import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

/**
 * 异步生产线程
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
public class AsyncProduceThread extends AbstractProduceThread {
    /**
     * 缓存长度
     * */
    private static final Integer CACHE_LENGTH = 1000000;

    public AsyncProduceThread(String brokerList) {
        super(brokerList);
    }

    @SneakyThrows
    @Override
    public void run() {
        List<Future<RecordMetadata>> list = new ArrayList<>();
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO,
                    "异步-" + super.sdf.format(new Date()));
            list.add(super.kafkaProducer.send(record));
            if (list.size() >= CACHE_LENGTH) {
                for (Future<RecordMetadata> future : list) {
                    future.get();
                }
                list.clear();
            }
            ProduceCounter.addAndGet(1, ProduceType.ASNYC);
        }
    }
}
