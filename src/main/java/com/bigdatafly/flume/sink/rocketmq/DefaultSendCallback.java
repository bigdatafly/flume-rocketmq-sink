/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;

/**
 * @author summer
 *
 */
public class DefaultSendCallback implements SendCallback{

	 
	/**
	 * 
	 */
	public DefaultSendCallback() {
		// TODO Auto-generated constructor stub
	}

	public void onSuccess(SendResult sendResult) {
		
		
	}

	public void onException(Throwable e) {
		
		
	}

}
