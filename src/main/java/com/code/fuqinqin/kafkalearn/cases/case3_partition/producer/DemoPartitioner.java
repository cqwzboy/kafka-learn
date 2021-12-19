package com.code.fuqinqin.kafkalearn.cases.case3_partition.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自定义分区器
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Slf4j
public class DemoPartitioner implements Partitioner {
    private AtomicLong partitionPosCounter = new AtomicLong(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        log.info("topic={}, key={}, keyBytes.size()={}, value={}, valueBytes.size()={}",
                topic, key, keyBytes == null ? 0 : keyBytes.length, value, valueBytes == null ? 0 : valueBytes.length);
        log.info("partitionsForTopic.size={}, availablePartitionsForTopic.size={}",
                cluster.partitionsForTopic(topic).size(), cluster.availablePartitionsForTopic(topic).size());
        log.info("cluster.partitionCountForTopic={}", cluster.partitionCountForTopic(topic));
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionNum = partitionInfos.size();
        if (keyBytes == null) {
            return (int) (partitionPosCounter.getAndAdd(1) % partitionNum);
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionNum;
        }
    }

    @Override
    public void close() {
        log.info("close DemoPartitioner...");
    }

    @Override
    public void configure(Map<String, ?> map) {
        log.info("config DemoPartitioner...");
    }
}
