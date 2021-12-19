package com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.impl;

import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceCounter;
import com.code.fuqinqin.kafkalearn.cases.case2_produce_model.produce.ProduceType;
import com.code.fuqinqin.kafkalearn.common.constant.TopicConstant;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;

/**
 * 异步生产线程
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
@Slf4j
public class AsyncProduceThread extends AbstractProduceThread {
    public AsyncProduceThread(String brokerList) {
        super(brokerList);
    }

    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicConstant.TOPIC_DEMO,
                    "异步-" + super.sdf.format(new Date()));
            // 回调函数Callback中的两个参数RecordMetadata和Exception是互斥的。
            // 当发送成功时，recordMetadata不为null，exception为null
            // 当发送失败时，recordMetadata为null，exception不为null
            super.kafkaProducer.send(record, (recordMetadata, exception) -> {
                if(exception != null){
                    log.error("消息发送失败", exception);
                }else{
                    ProduceCounter.addAndGet(1, ProduceType.ASNYC);
                }
            });
        }
    }
}
