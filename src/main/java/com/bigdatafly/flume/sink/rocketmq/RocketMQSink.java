/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.base.Preconditions;

/**
 * @author summer
 *
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class RocketMQSink extends AbstractSink implements Configurable {

	
	
	private String topic;
	
	private String producerGroup;
	
	private String tags;
	
	private String keys;
	
    private long timeout;
    
    private String clientClass;
    
    private MQProducer client;
    
    private String rocketSevrAddr;
    
    private int retryTimesWhenSendFailed;
    
    private boolean isTransaction = false;
    
    private boolean state = false; 
    
    private SendCallback sendCallback;
    
    private RPCHook hook;
    
    private SinkCounter sinkCounter;
    
    @Override
	public synchronized void start() {
		
		Preconditions.checkState(client != null, "No Client configured");
		try {
			client.start();
			state = true;
		} catch (MQClientException e) {
			state = false;
			e.printStackTrace();
			
		}
		
		sinkCounter.start();
		
		super.start();
	}
	
	public Status process() throws EventDeliveryException {
		
		Status state = Status.READY;
		Channel channel = getChannel();
		Transaction transcation = channel.getTransaction();
		Event event = null;
		try{
			
			transcation.begin();
			event = channel.take();
			if(event!=null){
				Message msg = new Message();
				client.send(msg,sendCallback, timeout);
				
			}else{
				state = Status.BACKOFF;
			}
			transcation.commit();
		}catch(Exception ex){
			transcation.rollback();
			if(ex instanceof MQClientException){
				
			}
			else if(ex instanceof RemotingException){
				
			}
			else{//InterruptedException
				throw new EventDeliveryException("Failed to send event to rocket mq: " + event, ex);
			}
		}finally{
			transcation.close();
		}
		
		return state;
	}

	public void configure(Context context) {
		
		this.producerGroup = context.getString(RocketMQSinkConstants.PRODUCER_GROUP, MixAll.DEFAULT_PRODUCER_GROUP);
		this.topic = context.getString(RocketMQSinkConstants.PRODUCER_TOPIC, RocketMQSinkConstants.DEFAULT_TOPIC);
		this.keys = context.getString(RocketMQSinkConstants.MESSAGE_KEYS, RocketMQSinkConstants.DEFAULT_MESSAGE_KEYS);
		this.clientClass = context.getString(RocketMQSinkConstants.CLIENT_TYPE, RocketMQSinkConstants.DEFAULT_CLIENT_TYPE);
		this.rocketSevrAddr = context.getString(RocketMQSinkConstants.SEVR_ADDR, RocketMQSinkConstants.DEFAULT_SEVR_ADDR);
		this.isTransaction = context.getBoolean(RocketMQSinkConstants.TRANSCATION_ENABLED, RocketMQSinkConstants.DEFAULT_TRANSCATION_ENABLED);
		this.timeout = context.getLong(RocketMQSinkConstants.SEND_MSG_TIME_OUT,RocketMQSinkConstants.DEFAULT_TIMEOUT);
		this.retryTimesWhenSendFailed = context.getInteger(RocketMQSinkConstants.SEND_FAILED_RETRY_TIMES,RocketMQSinkConstants.DEFAULT_SEND_FAILED_RETRY_TIMES);
		this.client = createRocketMQProducer();
		this.sendCallback = createDefaultSendCallback();
		this.hook = createDefaultRPCHook();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){

			 Object lock = new Object();
				public void run() {	
					synchronized(lock){
						if(state){
							client.shutdown();
							state = false;
						}
					}	
				}		    	
		    }));
		 
		 if(sinkCounter == null)
			 sinkCounter = new SinkCounter(getName());
			
	}

	
	private SendCallback createDefaultSendCallback(){
		
		return new DefaultSendCallback();
	}
	
	private RPCHook createDefaultRPCHook(){
		
		return new DefaultRPCHook();
	}
	
	private MQProducer createRocketMQProducer(){
		
		return getDefaultClient(producerGroup,hook);
	}
	
	@Override
	public synchronized void stop() {
		
		if(state){
			client.shutdown();
			state = false;
		}
		client = null;
		
		sinkCounter.stop();
		super.stop();
	}

	public MQProducer getDefaultClient( final String producerGroup,RPCHook hook){
		
		if(isTransaction)
			return new TransactionMQProducer(producerGroup,hook);
		else
			return new DefaultMQProducer(producerGroup,hook);
	}
}
