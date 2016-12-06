package com.github.harmishlakhani.rabbitmq.tutorial.two;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Worker 
{
	private final static String QUEUE_NAME = "hello";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws Exception
    {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(HOST);
    	
    	Connection connection = connectionFactory.newConnection();
    	final Channel channel = connection.createChannel();
    	
    	int prefetchCount = 1;
    	channel.basicQos(prefetchCount);
    	
    	boolean durable = true;
    	channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
    	System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    	
    	com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel){
    		 @Override
    	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
    	          throws IOException {
    	        String message = new String(body, "UTF-8");
    	        System.out.println(" [x] Received '" + message + "'");
    	        
    	        try {
    	        	doWork(message);
    	        } catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					System.out.println(" [x] Done");
					channel.basicAck(envelope.getDeliveryTag(), false);
    	        }
    	      }
    	};
    	boolean autoAck = false;
    	channel.basicConsume(QUEUE_NAME, autoAck, consumer);
    	
    	//channel.close();
    	//connection.close();
    }
    
    private static void doWork(String message) throws InterruptedException {
    	for(char ch : message.toCharArray()) {
    		if(ch == '.') {
    			Thread.sleep(10000);
    		}
    	}
		
	}
}
