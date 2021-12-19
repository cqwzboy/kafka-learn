/**
 * 拦截器（拦截器的触发时机在序列化器和分区器之前）
 *
 * ProducerInterceptor接口存在三个方法：
 *  onSend()，将消息序列化和计算分区之前被执行
 *  onAcknowledgement()，消息被执行onSend()、序列化和计算分区之后，消息被ACK和自定义的Callback之前；或者消息发送失败时被调用
 *  close()，在关闭拦截器时执行一些释放资源的工作
 * 这三个方法中抛出的异常都会被捕捉并记录到日志中，但并不会再向上传递
 *
 * @author fuqinqin
 * @date 2021-12-19
 * */
package com.code.fuqinqin.kafkalearn.cases.case4_interceptor.producer;