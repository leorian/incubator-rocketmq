package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * Created by xiezhonggui on 2017/5/27.
 */
public class BroadcastProducer {
    public static void main(String args[]) throws MQClientException, UnsupportedEncodingException,
            RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("leoRain_broadcast_producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        Message msg = new Message("HelloWorld" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        SendResult sendResult = producer.send(msg);
        System.out.println(sendResult);
        producer.shutdown();

    }
}
