package com.github.harmishlakhani.rabbitmq.tutorial.three;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer 
{
	private final static String EXCHANGE_NAME = "logs";
	private final static String HOST = "localhost";
	
    public static void main( String[] args ) throws IOException, TimeoutException
    {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(HOST);
    	
    	Connection connection = connectionFactory.newConnection();
    	Channel channel = connection.createChannel();
    	
    	channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    	
    	String message = getMessage();
    	channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
    	System.out.println(" [x] Sent '" + message + "'");
    	
    	channel.close();
    	connection.close();
    }

	private static String getMessage() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine();
	}
}
