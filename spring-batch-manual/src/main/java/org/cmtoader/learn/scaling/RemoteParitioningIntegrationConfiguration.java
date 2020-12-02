package org.cmtoader.learn.scaling;

import org.apache.logging.log4j.message.SimpleMessage;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.PollableChannel;

@Configuration
public class RemoteParitioningIntegrationConfiguration {

    // used for sending messages from master
    @Bean
    public MessagingTemplate messagingTemplate() {
        MessagingTemplate messagingTemplate = new MessagingTemplate(outboundRequests());
        messagingTemplate.setReceiveTimeout(60000000l);
        return messagingTemplate;
    }

    @Bean
    public DirectChannel outboundRequests() {
        return new DirectChannel();
    }

    // for every message coming in, it's sending a message via a rabbit mq
    @Bean
    @ServiceActivator(inputChannel = "outboundRequests")
    public AmqpOutboundEndpoint amqpOutboundEndpoint(AmqpTemplate template) {
        AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(template);

        endpoint.setExpectReply(true);
        endpoint.setOutputChannel(inboundRequests());

        // routes messages to this queue
        endpoint.setRoutingKey("partition.requests");

        return endpoint;
    }

    // rabbit mq queue definition
    @Bean
    public Queue requestQueue() {
        return new Queue("partition.requests", false);
    }

    @Bean
    @Profile("slave")
    public AmqpInboundChannelAdapter amqpInboundChannelAdapter(SimpleMessageListenerContainer listenerContainer) {
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
        adapter.setOutputChannel(inboundRequests());
        adapter.afterPropertiesSet();
        return adapter;
    }

    @Bean
    @Profile("slave")
    public SimpleMessageListenerContainer simpleMessageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(connectionFactory);
        listenerContainer.setQueueNames("partition.requests");
        listenerContainer.setAutoStartup(false);
        return listenerContainer;
    }

    @Bean
    public QueueChannel inboundRequests() {
        return new QueueChannel();
    }

    @Bean
    public PollableChannel outboundStaging() {
        return new NullChannel();
    }
}
