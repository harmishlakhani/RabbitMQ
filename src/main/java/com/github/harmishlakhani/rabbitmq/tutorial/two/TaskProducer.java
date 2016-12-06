package com.github.harmishlakhani.rabbitmq.tutorial.two;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class TaskProducer 
{
	private final static String QUEUE_NAME = "hello";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws IOException, TimeoutException
    {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(HOST);
    	
    	Connection connection = connectionFactory.newConnection();
    	Channel channel = connection.createChannel();
    	
    	boolean durable = true;
    	channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
    	
    	String message = getMessage();
    	// The empty string denotes the default or nameless exchange
    	channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
    	System.out.println(" [x] Sent '" + message + "'");
    	
    	channel.close();
    	connection.close();
    }

	private static String getMessage() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine();
	}
}
