package com.github.harmishlakhani.rabbitmq.tutorial.one;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Sender 
{
	private final static String QUEUE_NAME = "hello";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws IOException, TimeoutException
    {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(HOST);
    	
    	Connection connection = connectionFactory.newConnection();
    	Channel channel = connection.createChannel();
    	
    	channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    	String message = "Hi Harmish";
    	
    	//The empty string denotes the default or nameless exchange
    	channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
    	System.out.println(" [x] Sent '" + message + "'");
    	
    	channel.close();
    	connection.close();
    }
}
