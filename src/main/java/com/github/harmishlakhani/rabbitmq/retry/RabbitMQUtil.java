package com.github.harmishlakhani.rabbitmq.retry;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQUtil {

    public Channel getChannel(Connection connection, String exchangeName, String queueName, Map<String, Object> queueArgs) throws IOException {
    	Channel channel = connection.createChannel();
    	channel.exchangeDeclare(exchangeName, "direct");
    	channel.queueDeclare(queueName, true, false, false, queueArgs);
    	channel.queueBind(queueName, exchangeName, "");
    	return channel;
    }
    
    public Connection getConnection(String host) throws IOException, TimeoutException {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
    	connectionFactory.setHost(host);
    	return connectionFactory.newConnection();
    }
}
