package com.code.fuqinqin.kafkalearn.cases.case5_serializer_deserializer.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;

/**
 * 学生类消息序列化器
 *
 * @author fuqinqin
 * @date 2021-12-19
 */
@Slf4j
public class StudentSerializer implements Serializer<StudentDTO> {
    @Override
    public byte[] serialize(String topic, StudentDTO studentDTO) {
        if (studentDTO == null) {
            return null;
        }
        byte[] no, name, birthday;
        if (studentDTO.getNo() != null) {
            no = String.valueOf(studentDTO.getNo()).getBytes(Charset.forName("UTF-8"));
        } else {
            no = new byte[0];
        }
        if (studentDTO.getName() != null) {
            name = studentDTO.getName().getBytes(Charset.forName("UTF-8"));
        } else {
            name = new byte[0];
        }
        if (studentDTO.getBirthday() != null) {
            birthday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                    .format(studentDTO.getBirthday()).getBytes(Charset.forName("UTF-8"));
        } else {
            birthday = new byte[0];
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + 4 + no.length + name.length + birthday.length);
        byteBuffer.putInt(no.length);
        byteBuffer.put(no);
        byteBuffer.putInt(name.length);
        byteBuffer.put(name);
        byteBuffer.putInt(birthday.length);
        byteBuffer.put(birthday);
        byte[] bytes = byteBuffer.array();
        log.info("byteBuffer.length = {}", bytes.length);
        return bytes;
    }
}
