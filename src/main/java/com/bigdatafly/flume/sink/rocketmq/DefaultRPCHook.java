/**
 * 
 */
package com.bigdatafly.flume.sink.rocketmq;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @author summer
 *
 */
public class DefaultRPCHook implements RPCHook{

	public void doAfterResponse(String arg0, RemotingCommand arg1, RemotingCommand arg2) {
		// TODO Auto-generated method stub
		
	}

	public void doBeforeRequest(String arg0, RemotingCommand arg1) {
		// TODO Auto-generated method stub
		
	}

}
