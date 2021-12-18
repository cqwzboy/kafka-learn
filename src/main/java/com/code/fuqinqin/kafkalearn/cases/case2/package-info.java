/**
 * 比对kafka三种生产方式的性能差异
 * 三种方式：发后即忘（fire-and-forget），同步（sync）、异步（async）
 *
 * 结论：
 *  async缓存长度10		    800:4:1
 *  async缓存长度100	    800:15:1
 *  async缓存长度1000	    800:140:1
 *  async缓存长度10000	    800:600:1
 *  async缓存长度100000	    900:900:1
 *  async缓存长度1000000	1000:990:1
 *
 * @author fuqinqin
 * @date 2021-12-18
 * */
package com.code.fuqinqin.kafkalearn.cases.case2;