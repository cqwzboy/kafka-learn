package com.code.fuqinqin.kafkalearn.cases.case4_interceptor.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 添加前缀的生产者拦截器
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Slf4j
public class ProducePrefix2Interceptor implements ProducerInterceptor<String, String> {
    /**
     * 消息发送成功的统计项
     */
    private volatile AtomicLong successCounter = new AtomicLong(0);
    /**
     * 消息发送失败的统计项
     */
    private volatile AtomicLong failCounter = new AtomicLong(0);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix2-" + record.value();
        return new ProducerRecord<>(record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                modifiedValue,
                record.headers());
    }

    /**
     * 这个方法运行在Producer的I/O线程中，所以代码逻辑越简单越好，否则会影响消息的发送速度
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            successCounter.addAndGet(1);
        } else {
            failCounter.addAndGet(1);
        }
        double successRatio = (double) successCounter.get() / (successCounter.get() + failCounter.get());
        log.info("[拦截器2]发送成功={}条，发送失败={}条，发送成功率={}",
                successCounter.get(), failCounter.get(), successRatio * 100 + "%");
    }

    /**
     * 该方法当且仅当KafkaProducer执行close()方法时才会被执行
     */
    @Override
    public void close() {
        double successRatio = (double) successCounter.get() / (successCounter.get() + failCounter.get());
        log.info("[拦截器2-close]发送成功率={}", successRatio * 100 + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
