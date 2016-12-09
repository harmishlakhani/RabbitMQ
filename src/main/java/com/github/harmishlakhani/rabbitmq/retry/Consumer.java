package com.github.harmishlakhani.rabbitmq.retry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Consumer 
{
	private final static String WORK_EXCHANGE_NAME = "WorkExchange";
	private final static String WORK_QUEUE_NAME = "WorkQueue";
	private final static String RETRY_EXCHANGE_NAME = "RetryExchange";
	private final static String RETRY_QUEUE_NAME = "RetryQueue";
	private final static Integer TTL = 60000;
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws Exception
    {
    	RabbitMQUtil rabbitMQUtil = new RabbitMQUtil();
    	Connection connection = rabbitMQUtil.getConnection(HOST);
    	Channel workChannel = rabbitMQUtil.getChannel(connection, WORK_EXCHANGE_NAME, WORK_QUEUE_NAME, null);
    	
    	Map<String, Object> queueArgs = new HashMap<String, Object>();
    	queueArgs.put("x-dead-letter-exchange", WORK_EXCHANGE_NAME);
    	queueArgs.put("x-message-ttl", TTL);
    	final Channel retryChannel = rabbitMQUtil.getChannel(connection, RETRY_EXCHANGE_NAME, RETRY_QUEUE_NAME, queueArgs);
    	
    	System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    	
    	com.rabbitmq.client.Consumer consumer = new DefaultConsumer(workChannel){
    		 @Override
    	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
    	          throws IOException {
    			String message = new String(body, "UTF-8");
    	        System.out.println(" [x] Received '" + message + "'");
    	        boolean isProcesed = processMessage(message);
    	        if(!isProcesed) {
    	        	publishRetryMessage(retryChannel, message);
    	        }
    	        }
    	};
    	workChannel.basicConsume(WORK_QUEUE_NAME, true, consumer);
    }

    protected static void publishRetryMessage(Channel channel, String message) throws IOException {
    	channel.basicPublish(RETRY_EXCHANGE_NAME, "", null, message.getBytes());
	}
    
	protected static boolean processMessage(String message) {
		System.out.println("Processing message...: " + message);
		return true;
	}
}
