/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

import java.util.List;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author summer
 *
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class RocketMQSink extends AbstractSink implements Configurable {

	private static Logger logger = LoggerFactory.getLogger
			(RocketMQSink.class);
	
	private String topic;
	
	private String producerGroup;
	
	private String tags;
	
	private String keys;
	
    private long timeout;
    
    private int batchSize;
    
    private String clientClass;
    
    private DefaultMQProducer client;
    
    private String namesrvAddr;
    
    private int retryTimesWhenSendFailed;
    
    private boolean isTransaction = false;
    
    private boolean state = false; 
    
    private SendCallback sendCallback;
    
    private RPCHook hook;
    
    private SinkCounter sinkCounter;
    
    @Override
	public synchronized void start() {
		
		Preconditions.checkState(client != null, "No Client configured");
		
		try{
			startRocketMQ();
		}catch(MQClientException ex ){
			
		}
		
		ServiceState serviceState = getServiceState();
		Preconditions.checkState(serviceState == ServiceState.RUNNING, "No Client configured");
		
		sinkCounter.start();
		super.start();
	}
	
    private void startRocketMQ() throws MQClientException{
    	
    	try {
    		client.setNamesrvAddr(namesrvAddr);
    		client.setSendMsgTimeout((int)timeout);
    		client.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);
    		client.setRetryAnotherBrokerWhenNotStoreOK(true);
    		
			client.start();
			state = true;
		} catch (MQClientException e) {
			state = false;
			throw e;
			
		}
    }
    
    protected String getHeader(Map<String,String> headers,String key,String defaultValue){
    	
    	if(headers!=null &&
    			!headers.isEmpty() && 
    			headers.containsKey(key)){
    		return headers.get(key);
    	}
    	
    	return defaultValue;
    }
    
    protected Message serializer(Event event){
    	
    	Map<String,String> headers = event.getHeaders();
    	String tempTopic = topic;
    	
    	if(RocketMQSinkConstants.DEFAULT_TOPIC.equals(tempTopic))
    		topic = getHeader(headers,RocketMQSinkConstants.PRODUCER_TOPIC,tempTopic);
    	String eventkeys = getHeader(headers,RocketMQSinkConstants.MESSAGE_KEYS,keys);
    	byte[] body = event.getBody();
    	
        if (logger.isDebugEnabled()) {
            logger.debug("{Event} " + topic + " : " + eventkeys + " : "
              + new String(body));
            
          }
    	
    	Message msg = new Message(topic,tags,keys,body);

    	return msg;
    }
    
    private int getBatchSize(){
    	
    	return batchSize;
    }
    
	public Status process() throws EventDeliveryException {
		
		Status state = Status.READY;
		Channel channel = getChannel();
		Transaction transcation = channel.getTransaction();
		Event event = null;
		try{
			
			transcation.begin();
			List<Event> batch = Lists.newLinkedList();
			for (int i = 0; i < getBatchSize(); i++) {
		        event = channel.take();
		        if (event == null) {
		          break;
		        }

		        batch.add(event);
		    }
			
			int size = batch.size();
			int batchSize = getBatchSize();
			
			if(size == 0){
				sinkCounter.incrementBatchEmptyCount();
				state = Status.BACKOFF;
				
			}else{
				
				if (size < batchSize) {
			          sinkCounter.incrementBatchUnderflowCount();
			    } else {
			          sinkCounter.incrementBatchCompleteCount();
			    }
			    sinkCounter.addToEventDrainAttemptCount(size);
			       
				
				for(int i=0;i<size;i++){
					Message msg = serializer(batch.get(i));
					client.send(msg,sendCallback, timeout);
				}
			    
			}
			transcation.commit();
		}catch(Throwable ex){
			transcation.rollback();
			
			if(ex instanceof Error){
				
				if(logger.isErrorEnabled())
					logger.error("RocketMQ Sink " + getName() + ": Unable to get event from" +
				            " channel " + channel.getName() + ". Exception follows.", ex);
				
				throw (Error)ex;
			}else if(ex instanceof MQClientException){
				if(logger.isErrorEnabled())
					logger.error("RocketMQ Sink " + getName() + ": Unable to send event to" +
				            " rocketmq topic: " + topic + ". Exception follows.", ex);
				state = Status.BACKOFF;
			}
			else if(ex instanceof RemotingException){
				
				if(logger.isErrorEnabled())
					logger.error("RocketMQ Sink " + getName() + ": Unable to send event to" +
				            " rocketmq topic: " + topic + ". Exception follows.", ex);
				state = Status.BACKOFF;
			}
			else{//InterruptedException
				throw new EventDeliveryException("Failed to send event to rocket mq: " + event, ex);
			}
			
			sinkCounter.incrementConnectionFailedCount();
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
		this.namesrvAddr = context.getString(RocketMQSinkConstants.SEVR_ADDR, RocketMQSinkConstants.DEFAULT_SEVR_ADDR);
		this.isTransaction = context.getBoolean(RocketMQSinkConstants.TRANSCATION_ENABLED, RocketMQSinkConstants.DEFAULT_TRANSCATION_ENABLED);
		this.timeout = context.getLong(RocketMQSinkConstants.SEND_MSG_TIME_OUT,RocketMQSinkConstants.DEFAULT_TIMEOUT);
		this.retryTimesWhenSendFailed = context.getInteger(RocketMQSinkConstants.SEND_FAILED_RETRY_TIMES,RocketMQSinkConstants.DEFAULT_SEND_FAILED_RETRY_TIMES);
		this.batchSize = context.getInteger(RocketMQSinkConstants.BATCH_SIZE,RocketMQSinkConstants.DEFAULT_BATCH_SIZE);
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
		
		if(topic.equals(RocketMQSinkConstants.DEFAULT_TOPIC)) {
		      logger.warn("The Property 'topic' is not set. " +
		        "Using the default topic name: " +
		        RocketMQSinkConstants.DEFAULT_TOPIC);
		    } else {
		      logger.info("Using the static topic: " + topic +
		        " this may be over-ridden by event headers");
		  }
		
		 if(sinkCounter == null)
			 sinkCounter = new SinkCounter(getName());
			
	}

	
	private SendCallback createDefaultSendCallback(){
		
		return new DefaultSendCallback();
	}
	
	private RPCHook createDefaultRPCHook(){
		
		return new DefaultRPCHook();
	}
	
	private DefaultMQProducer createRocketMQProducer(){
		
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
		
		logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), sinkCounter);
		super.stop();
	}

	public ServiceState getServiceState(){
		
		ServiceState state = ServiceState.CREATE_JUST;
		state =  client.getDefaultMQProducerImpl().getServiceState();
		return state;
		
	}
	
	public DefaultMQProducer getDefaultClient( final String producerGroup,RPCHook hook){
		
		if(RocketMQSinkConstants.DEFAULT_CLIENT_TYPE.equals(clientClass))
		    ;
		if(isTransaction)
			return new TransactionMQProducer(producerGroup,hook);
		else
			return new DefaultMQProducer(producerGroup,hook);
	}
	
	
}
