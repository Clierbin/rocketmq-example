package com.gupaovip.rocketmq.distributedtransaction;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * ClassName:TransactionConsumer
 * Package:com.gupaovip.rocketmq.distributedtransaction
 * description
 * Created by zhangbin on 2019/9/6.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/9/6 15:41
 */
public class TransactionConsumer {
    public static void main(String[] args) throws MQClientException {
        // rocketMQ 出了在同一组合不同组之间的消费者的特性和kafka相同之外
        // RocketMQ 可以支持广播消息,就意味着,同一个group的每个消费者都可以消费用一个消息
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_gp");
        consumer.setNamesrvAddr("192.168.5.179:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // subExpression : 订阅表达式。只支持或操作如“tag1 || tag2 || tag3”<br> *如果为null或*表达式，意味着订阅全部
        // 可以支持sql的表达式  or and  a=? ...
        consumer.subscribe("order_tx_topic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach(message -> {
                    // 扣减库存
                    System.out.println("开始业务处理逻辑消息体: " + new String(message.getBody() + "key:" +message.getKeys()) );

                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

    }
}
