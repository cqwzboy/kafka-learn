package com.code.fuqinqin.kafkalearn.common.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * @author fuqinqin
 * @date 2021-12-18
 */
public class Configuration {

    /**
     * 构建kafka消费配置文件
     *
     * @return Properties
     */
    public static Properties buildConsumerConfig(String brokerList, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    /**
     * 构建kafka生产配置文件
     *
     * @return Properties
     */
    public static Properties buildProducerConfig(String brokerList) {
        return buildProducerConfig(brokerList, null, null, null);
    }

    public static Properties buildProducerConfig(String brokerList, Class<? extends Partitioner> partitioner) {
        return buildProducerConfig(brokerList, partitioner, null, null);
    }

    public static Properties buildProducerConfig(String brokerList,
                                                 Class<? extends Partitioner> partitioner,
                                                 List<Class<? extends ProducerInterceptor<?,?>>> producerInterceptors,
                                                 Class<? extends Serializer<?>> valueSerializer) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 默认
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 默认
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 当发生可重试的异常时，若配置了重试次数大于0，则会进行重试，如果重试成功将不抛异常
        // 可重试异常：NetworkException，LeaderNotAvailableException，UnknownTopicOrPartitionException，NotEnoughReplicasException，NotCoordinatorException
        // 不可重试异常：RecordTooLargeException
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);
        // 自定义分区器
        if (partitioner != null) {
            properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.getName());
        }
        // 自定义生产者拦截器
        if (producerInterceptors != null && producerInterceptors.size() > 0) {
            StringBuilder buffer = new StringBuilder();
            for (Class<? extends ProducerInterceptor<?, ?>> interceptor : producerInterceptors) {
                buffer.append(interceptor.getName()).append(",");
            }
            String interceptors = buffer.toString().substring(0, buffer.length() - 1);
            properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        }
        // 消息序列化器
        if (valueSerializer != null) {
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        }
        return properties;
    }
}
