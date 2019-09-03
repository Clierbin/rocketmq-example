package com.gupaovip.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * ClassName:MyRocketMQProducer
 * Package:com.gupaovip.rocketmq.demo
 * description
 * Created by zhangbin on 2019/9/3.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/9/3 10:28
 */
public class MyRocketMQConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("gp_consumer_group");
        consumer.setNamesrvAddr("192.168.5.179:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("gp_test_topic", "*");
        // 并行消费
/*        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("Receive Message" + list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 签收
            }
        });*/
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                MessageExt messageExt = list.get(0);
                // Clierbin Throw Exception
                // 重新发送
                // DLQ (死信队列,通用设计)
                if (messageExt.getReconsumeTimes() == 3) {// 消息重发了三次
                    // 持久化 消息记录表
                    return ConsumeOrderlyStatus.SUCCESS;//签收
                }
                return ConsumeOrderlyStatus.SUCCESS;//签收
            }
        });
        consumer.start();
    }
}
