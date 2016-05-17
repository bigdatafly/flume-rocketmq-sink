/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.alibaba.rocketmq.client.producer.MQProducer;

/**
 * @author summer
 *
 */
public class RocketMQSink extends AbstractSink implements Configurable{

	
	MQProducer client;
	
	//维护clients的状态
	private Map<String,Integer> clientsStates;
	
	
	
	@Override
	public synchronized void start() {
		
		
	    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){

			public void run() {
				
				
			}
	    	
	    }));
		
		super.start();
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	public Status process() throws EventDeliveryException {
		
		return null;
	}

	public void configure(Context context) {
		
		
		
	}
	
	private void createConnection(){
		
	}
	
	private void resetConnection(){
		
	}
	
	private void destroyConnection(){
		
	}
	
	private void verifyConnection() throws FlumeException {
	    if (client == null) {
	      createConnection();
	    } else if (!client.isActive()) {
	      destroyConnection();
	      createConnection();
	    }
	}
	
	
}
