/**
 * 比对kafka三种生产方式的性能差异
 * 三种方式：发后即忘（fire-and-forget），同步（sync）、异步（async）
 *
 * 结论（运行稳定后的组略值）：
 *  fireAndForget : async : sync = 546 : 589 : 1
 *
 * @author fuqinqin
 * @date 2021-12-18
 * */
package com.code.fuqinqin.kafkalearn.cases.case2_produce_model;