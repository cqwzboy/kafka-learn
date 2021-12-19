package com.code.fuqinqin.kafkalearn.cases.case5_serializer_deserializer.producer;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 学生类，Kafka消息传输类
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Data
public class StudentDTO implements Serializable {
    private Long no;
    private String name;
    private Date birthday;
}
