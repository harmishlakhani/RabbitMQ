package com.github.harmishlakhani.rabbitmq.tutorial.five;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Consumer 
{
	private final static String EXCHANGE_NAME = "topic_logs";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws Exception
    {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(HOST);
    	
    	Connection connection = connectionFactory.newConnection();
    	Channel channel = connection.createChannel();
    	
    	channel.exchangeDeclare(EXCHANGE_NAME, "topic");
    	String queueName = channel.queueDeclare().getQueue();
    	
    	if (args.length < 1){
    		System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
    	    System.exit(1);
    	}
    	
    	for(String bindingKey : args){
    	    channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    	}
    	
    	System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    	
    	com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel){
    		 @Override
    	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
    	          throws IOException {
    			String message = new String(body, "UTF-8");
    	        System.out.println(" [x] Received '" + message + "'");
    	        }
    	};
    	channel.basicConsume(queueName, true, consumer);
    	
    	//channel.close();
    	//connection.close();
    }
}
