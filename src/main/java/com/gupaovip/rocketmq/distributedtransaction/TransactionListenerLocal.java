package com.gupaovip.rocketmq.distributedtransaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName:TransactionListenerLocal
 * Package:com.gupaovip.rocketmq.distributedtransaction
 * description
 * Created by zhangbin on 2019/9/6.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/9/6 15:42
 */
public class TransactionListenerLocal implements TransactionListener {
    private Map<String,Boolean> results=new ConcurrentHashMap<>();
    /**
     * 执行本地事务
     *
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println("开始执行本地事务: " + o.toString()); // o
        String orderId = o.toString();
        // 模拟数据库保存 (成功/失败)
        boolean result = Math.abs(Objects.hash(orderId)) % 2 == 0;
        results.put(orderId,result); // 可以回查
        return result ? LocalTransactionState.COMMIT_MESSAGE
                : LocalTransactionState.UNKNOW;
    }

    /**
     * 反查询  提供给事务执行状态检查的回调方法,给broker用的
     *
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String orderId=messageExt.getKeys();
        Boolean result = results.get(orderId);
        System.out.println("执行事务回调检查 : orderId : " + orderId +"---------"+result.toString());
        System.out.println("数据的处理结果 : " + result.toString());  // 只有成功或则失败
        return result ? LocalTransactionState.COMMIT_MESSAGE :
                LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
