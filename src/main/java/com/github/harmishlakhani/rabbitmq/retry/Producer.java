package com.github.harmishlakhani.rabbitmq.retry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class Producer 
{
	private final static String WORK_EXCHANGE_NAME = "WorkExchange";
	private final static String WORK_QUEUE_NAME = "WorkQueue";
	private final static String RETRY_EXCHANGE_NAME = "RetryExchange";
	private final static String RETRY_QUEUE_NAME = "RetryQueue";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws IOException, TimeoutException
    {
    	RabbitMQUtil rabbitMQUtil = new RabbitMQUtil();
    	Connection connection = rabbitMQUtil.getConnection(HOST);
    	Channel workChannel = rabbitMQUtil.getChannel(connection, WORK_EXCHANGE_NAME, WORK_QUEUE_NAME, null);
    	try {
    		String message = getMessage();
    		workChannel.basicPublish(WORK_EXCHANGE_NAME, "", null, message.getBytes());
        	System.out.println(" [x] Sent '" + message + "'");
    	} finally {
    		workChannel.close();
    		connection.close();
    	}
    }
    
	private static String getMessage() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine();
	}
}
