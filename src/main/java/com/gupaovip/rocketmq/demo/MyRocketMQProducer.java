package com.gupaovip.rocketmq.demo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

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
public class MyRocketMQProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 事务消息的时候会用到
        DefaultMQProducer producer = new DefaultMQProducer("gp_producer_group");
        producer.setNamesrvAddr("192.168.5.179:9876"); // 它会从命名服务器上拿到broker的地址
        producer.start();
        int num=0;
        while (num<20){
            num++;
            // Topic
            // tags -> 标签 (分类) -> (筛选)
            Message message=new Message("gp_test_topic","TagA",("Producer"+num).getBytes());

//            SendResult sendResult = producer.send(message);// 是否阻塞
//            System.out.println(sendResult);
           /* producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable e) {

                }
            });*/
            // 发送到指定queue
           producer.send(message, new MessageQueueSelector() {
               @Override
               public MessageQueue select(List<MessageQueue> list, Message msg, Object arg) {
                   return list.get(0);
               }
           },"key-"+num);
        }
        // 异步就不能关闭了
        // producer.shutdown();
    }
}
