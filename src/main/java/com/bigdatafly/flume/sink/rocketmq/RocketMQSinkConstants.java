/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

/**
 * @author summer
 *
 */
public class RocketMQSinkConstants {

	  //public static final String TIMESTAMP = "timestamp";
	public static final String BATCH_SIZE = "batchSize";
	  //public static final String BATCH_DURATION_MS = "batchDurationMillis";
    public static final String SEND_MSG_TIME_OUT = "sendtimeout";
	public static final String CLIENT_TYPE = "clienttype";
	public static final String SEVR_ADDR = "sevraddr";
	public static final String PRODUCER_GROUP ="productgroup";
	public static final String PRODUCER_TOPIC = "topic";
	public static final String TRANSCATION_ENABLED = "istranscation";
	public static final String SEND_FAILED_RETRY_TIMES = "sendfailedretrytimes";
	
	public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
	public static final String DEFAULT_TOPIC = "TOPIC";
	public static final String DEFAULT_SEVR_ADDR = "localhost:8080";
	public static final String DEFAULT_CLIENT_TYPE = "default";
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final long   DEFAULT_TIMEOUT = 10;
	public static final int	   DEFAULT_SEND_FAILED_RETRY_TIMES = 10;
	
	public static final int DEFAULT_BATCH_SIZE = 1000;
	//public static final int DEFAULT_BATCH_DURATION = 1000;

	public static final boolean DEFAULT_TRANSCATION_ENABLED =  false;
	public static final String MESSAGE_KEYS = "msgkey";
	public static final String DEFAULT_MESSAGE_KEYS = "";
	  
}
