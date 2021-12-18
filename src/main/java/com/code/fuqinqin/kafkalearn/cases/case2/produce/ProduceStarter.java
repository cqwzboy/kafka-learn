package com.code.fuqinqin.kafkalearn.cases.case2.produce;

import com.code.fuqinqin.kafkalearn.cases.case2.produce.impl.AsyncProduceThread;
import com.code.fuqinqin.kafkalearn.cases.case2.produce.impl.FireAndForgetProduceThread;
import com.code.fuqinqin.kafkalearn.cases.case2.produce.impl.SyncProduceThread;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 生产者启动类
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
@Component
public class ProduceStarter {
    @Value("${kafka.broker.list}")
    private String brokerList;

    @PostConstruct
    public void start() {
        new Thread(new FireAndForgetProduceThread(brokerList)).start();
        new Thread(new SyncProduceThread(brokerList)).start();
        new Thread(new AsyncProduceThread(brokerList)).start();
    }
}
