package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * Created by xiezhonggui on 2017/5/27.
 */
public class P2PConsumer02 {
    public static void main(String args[]) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("leoRain-XX-XXFDD");
        //consumer.setMessageModel(MessageModel.BROADCASTING);
        //consumer.setMessageModel(MessageModel.CLUSTERING);
        //consumer.setMessageModel(MessageModel.BROADCASTING);
         consumer.setMessageModel(MessageModel.CLUSTERING);
        //consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setNamesrvAddr("172.16.150.178:9876;172.16.150.143:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        //consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()-1000*60*60*24));

        //consumer.setConsumeTimestamp((System.currentTimeMillis() +(1000*24*60*60)) +"");

        consumer.subscribe("XZG", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");

    }
}
