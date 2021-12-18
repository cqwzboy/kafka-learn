package com.code.fuqinqin.kafkalearn.cases.case2.produce;

import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生产统计
 *
 * @author fuqinqin
 * @date 2021-12-18
 */
@Slf4j
public class ProduceCounter {
    private static final AtomicLong fireAndForgetCounter = new AtomicLong(0);
    private static final AtomicLong syncCounter = new AtomicLong(0);
    private static final AtomicLong asyncCounter = new AtomicLong(0);
    private static int statisticsCount = 0;

    /**
     * 统计kafka生产数据量
     *
     * @param gap
     * @param produceType
     * @return long
     */
    public static long addAndGet(int gap, ProduceType produceType) {
        if (produceType == null) {
            return -1;
        }
        switch (produceType) {
            case FIRE_AND_FORGET:
                return fireAndForgetCounter.addAndGet(gap);
            case SYNC:
                return syncCounter.addAndGet(gap);
            default:
                return asyncCounter.addAndGet(gap);
        }
    }

    static {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("---------------------------- {} ---------------------------", ++statisticsCount);
                long fireAndForget = fireAndForgetCounter.get();
                long async = asyncCounter.get();
                long sync = syncCounter.get();
                log.info("fireAndForgetCounter = {}", fireAndForget);
                log.info("asyncCounter = {}", async);
                log.info("syncCounter = {}", sync);
                log.info("fireAndForget : async : sync = {} : {} : {}", fireAndForget / sync, async / sync, 1);
            }
        }, 5000, 5000);
    }
}
