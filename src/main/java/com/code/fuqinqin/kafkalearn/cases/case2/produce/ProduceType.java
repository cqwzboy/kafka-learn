package com.code.fuqinqin.kafkalearn.cases.case2.produce;

/**
 * kafka生产类型
 *
 * @author fuqinqin
 * @date 2021-12-18
 * */
public enum ProduceType {
    /**发后即忘*/
    FIRE_AND_FORGET,
    /**同步*/
    SYNC,
    /**异步*/
    ASNYC,
    ;
}
