package com.gupaovip.rocketmq.distributedtransaction;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ClassName:TransactionProducer
 * Package:com.gupaovip.rocketmq.demo.distributedtransaction
 * description
 * Created by zhangbin on 2019/9/6.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/9/6 15:41
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 创建一个事务MQ生产者
        TransactionMQProducer transactionMQProducer = new
                TransactionMQProducer("transaction_gp");
        transactionMQProducer.setNamesrvAddr("192.168.5.179:9876");
        // 创建一个固定数量的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        transactionMQProducer.setExecutorService(executorService);
        // 本地事务的监听
        transactionMQProducer.setTransactionListener(new TransactionListenerLocal());

        transactionMQProducer.start();

        for (int i = 0; i < 20; i++) {
            String ordId= UUID.randomUUID().toString();
            String body="{'operation':'doOrder','orderId':'"+ordId+"'}";
            Message message=new Message("order_tx_topic","TagA",ordId+"&"+i,body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            transactionMQProducer.sendMessageInTransaction(message,ordId+"&"+i);
            Thread.sleep(1000);
        }
    }
}
