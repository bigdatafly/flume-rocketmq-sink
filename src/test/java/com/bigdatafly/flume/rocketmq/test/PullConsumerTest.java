/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bigdatafly.flume.rocketmq.test;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


import java.util.*;

import org.junit.Before;
import org.junit.Test;


/**
 * PullConsumer，订阅消息
 */
public class PullConsumerTest {
    DefaultMQPullConsumer consumer;
    MessageQueue mq;
    OffsetStore offsetStore;

    @Before
    public void initEnv() {
        consumer = new DefaultMQPullConsumer("PushConsumer");
        consumer.setNamesrvAddr("172.27.101.67:9876");
        mq = new MessageQueue();
        mq.setQueueId(0);
        mq.setTopic("Topic3");
        mq.setBrokerName("broker-a");
        offsetStore = consumer.getDefaultMQPullConsumerImpl().getOffsetStore();

        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试尝试更新offerset
     */
    @Test
    public void testModify() {
        try {
            long l = consumer.fetchConsumeOffset(mq, false);
            System.out.println("得到offerset" + l);
            //更新offerset
            consumer.updateConsumeOffset(mq, l + 1);
            OffsetStore offsetStore = consumer.getDefaultMQPullConsumerImpl().getOffsetStore();
            offsetStore.persist(mq);
            l = consumer.fetchConsumeOffset(mq, false);
            System.out.println("更新后的offerset" + l);

        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试获取队列中的数据
     */
    @Test
    public void testModifyMessage() {
        long offset = 0;
        try {
            PullResult pullResult = consumer.pull(mq, null, offset, 9);
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            for (MessageExt ms : msgFoundList) {
                System.out.println(ms.getReconsumeTimes());
                ms.setReconsumeTimes(ms.getReconsumeTimes());
                long queueOffset = ms.getQueueOffset();
                consumer.sendMessageBack(ms, 1);
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 实时更新offerset
     */
    @Test
    public void testRecodeMessage() {
        long offset = 0;
        try {
            PullResult pullResult = consumer.pull(mq, null, offset, 9);
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            for (MessageExt ms : msgFoundList) {
                long queueOffset = ms.getQueueOffset();
                saveOfferSet(queueOffset, offsetStore, mq);
            }
            //更新offerset
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新offerset
     *
     * @param offerSet    更新值
     * @param offsetStore offsetStore
     * @param mq          需要更新的队列
     * @throws MQClientException
     */
    public void saveOfferSet(long offerSet, OffsetStore offsetStore, MessageQueue mq) throws MQClientException {
        System.out.println("当前offerset" + offerSet);
        consumer.updateConsumeOffset(mq, offerSet);
        offsetStore.persist(mq);
    }

    /**
     * 查看mq的offersets
     *
     * @throws MQClientException
     */
    @Test
    public void lookOfferSet() throws MQClientException {
        System.out.println(consumer.fetchConsumeOffset(mq, true));
    }

    @Test
    public void testget() throws MQClientException {
        try {

            long l = consumer.fetchConsumeOffset(mq, false);
            System.out.println("-----------" + l);

            PullResult pr = null;


            long beginTime = System.currentTimeMillis();

            consumer.updateConsumeOffset(mq, 9);
            l = consumer.fetchConsumeOffset(mq, false);
            System.out.println("-----------" + l);
            long offset = 0;
            PullResult pullResult = consumer.pull(mq, null, offset, 9);

//            QueryResult topic2 = consumer.queryMessage("Topic2", 25 + "", 1, 2, 3);
            MessageExt ac1B654300002A9F0000000000022E35 = consumer.viewMessage("AC1B654300002A9F0000000000022E35");
            System.out.println("this is the " + new String(ac1B654300002A9F0000000000022E35.getBody())); //Hello RocketMQ 29
            HashMap<String, MessageExt> success = new HashMap<String, MessageExt>();
            System.out.println(System.currentTimeMillis() - beginTime);
            System.out.println(pullResult);
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            List<MessageExt> msgFail = new ArrayList<MessageExt>();
            Random random = new Random();
            for (MessageExt ms : msgFoundList) {
                int reconsumeTimes = ms.getReconsumeTimes();
                System.out.println("this is " + reconsumeTimes);
                ms.setReconsumeTimes(reconsumeTimes + 1);
                int i = random.nextInt();
                if ((i & 2) == 0) {
                    System.out.println("事物成功");
                    success.put(ms.getMsgId(), ms);
                } else {
                    System.out.println("事物失败");
                    msgFail.add(ms);
                }
            }
            System.out.println("第一次执行失败" + msgFail.size() + "个");

            boolean check = check(msgFoundList, success);
            if (!check) {
                while (msgFail.size() != 0) {
                    for (int i = 0; i < msgFail.size(); i++) {
                        int rd = random.nextInt() % 2;
                        if ((rd & 2) == 0) {
                            System.out.println("执行成功");
                            success.put(msgFail.get(i).getMsgId(), msgFail.get(i));
                            msgFail.remove(i);
                            System.out.println("还剩下" + msgFail.size() + "个事件");
                        } else {
                            System.out.println("执行失败");
                            System.out.println("还剩下" + msgFail.size() + "个事件");
                        }
                    }
                    System.out.println("等待1000毫秒后重试");
                    Thread.sleep(1000);
                }
            }
            System.out.println("一共执行" + msgFoundList.size() + "个事件");
            System.out.println("执行成功" + success.size() + "个");
            //QueueOffsetCache.putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
            System.out.println(consumer.maxOffset(mq));
        } catch (Exception e) {
            e.printStackTrace();
        }

        consumer.shutdown();
    }

    public boolean check(List<MessageExt> msgFoundList, HashMap<String, MessageExt> success) {
        return success.size() == msgFoundList.size();
    }
    
    public static void main(String[] args) throws MQClientException {
    	  /**
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
         * 注意：ConsumerGroupName需要由应用来保证唯一
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("productgroup");
        consumer.setNamesrvAddr("172.27.101.67:9876");
        consumer.subscribe("Topic3", "*");

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				
				System.out.println(msgs);
				
				
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
        });

        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         */
        consumer.start();

        System.out.println("Consumer Started.");
    }
}
