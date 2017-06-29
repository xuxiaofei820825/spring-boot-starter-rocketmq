package com.github.rocketmq.config;

import java.util.List;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.rocketmq.event.RocketMqEvent;

@Configuration
@EnableConfigurationProperties(RocketMqProperties.class)
public class RocketMqConfiguration {

	/** logger */
	private final static Logger logger = LoggerFactory.getLogger( RocketMqConfiguration.class );

	@Autowired
	private RocketMqProperties properties;

	@Autowired
	private ApplicationEventPublisher publisher;

	/**
	 * 不加载单独的配置文件。直接使用Spring boot的配置
	 */
	static {
		System.setProperty( "rocketmq.client.log.loadconfig", "false" );
	}

	/**
	 * 发送普通消息
	 */
	@Bean(name = "default")
	@ConditionalOnProperty("spring.rocketmq.defaultProducer")
	public DefaultMQProducer defaultMQProducer() throws MQClientException {
		DefaultMQProducer producer = new DefaultMQProducer( properties.getDefaultProducer() );
		producer.setNamesrvAddr( properties.getNamesrvAddr() );
		producer.setInstanceName( properties.getInstanceName() );
		producer.setVipChannelEnabled( false );
		producer.start();

		// info log
		logger.info( "Default message producer is started." );

		return producer;
	}

	/**
	 * 发送事务消息
	 */
	@Bean(name = "trans")
	@ConditionalOnProperty("spring.rocketmq.transProducer")
	public TransactionMQProducer transactionMQProducer() throws MQClientException {
		TransactionMQProducer producer = new TransactionMQProducer( properties.getTransProducer() );
		producer.setNamesrvAddr( properties.getNamesrvAddr() );
		producer.setInstanceName( properties.getInstanceName() );
		producer.setTransactionCheckListener( ( MessageExt msg ) -> {
			// info log
			logger.info( "Message checking......" );

			return LocalTransactionState.COMMIT_MESSAGE;
		} );
		// 事务回查最小并发数
		producer.setCheckThreadPoolMinSize( 2 );
		// 事务回查最大并发数
		producer.setCheckThreadPoolMaxSize( 5 );
		// 队列数
		producer.setCheckRequestHoldMax( 2000 );
		producer.start();

		// info log
		logger.info( "Transaction message producer is started." );

		return producer;
	}

	/**
	 * 消费者
	 */
	@Bean
	@ConditionalOnProperty("spring.rocketmq.defaultConsumer")
	public DefaultMQPushConsumer pushConsumer() throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer( properties.getDefaultConsumer() );
		Set<String> setTopic = properties.getDefaultTopic();
		for ( String topic : setTopic ) {
			System.out.println( topic );
			consumer.subscribe( topic, "*" );
		}
		consumer.setNamesrvAddr( properties.getNamesrvAddr() );
		consumer.setConsumeFromWhere( ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET );
		consumer.setConsumeMessageBatchMaxSize( 1 );

		consumer.registerMessageListener( new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage( List<MessageExt> msgs, ConsumeConcurrentlyContext context ) {
				return null;
			}
		} );

		consumer.registerMessageListener( ( List<MessageExt> msgs, ConsumeConcurrentlyContext context ) -> {
			MessageExt msg = msgs.get( 0 );
			try {
				publisher.publishEvent( new RocketMqEvent( msg, consumer ) );
			}
			catch ( Exception e ) {
				if ( msg.getReconsumeTimes() <= 1 ) {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				else {
					System.out.println( "定时重试！" );
				}
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		} );
		new Thread( () -> {
			try {
				Thread.sleep( 3000 );
				try {
					consumer.start();
					System.out.println( "普通消费开启" );
				}
				catch ( MQClientException e ) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			catch ( InterruptedException e ) {
				e.printStackTrace();
			}
		} ).start();
		return consumer;
	}

	/**
	 * 顺序消费者
	 */
	@Bean
	@ConditionalOnProperty("spring.rocketmq.orderConsumer")
	public DefaultMQPushConsumer pushOrderConsumer() throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer( properties.getOrderConsumer() );
		Set<String> setTopic = properties.getOrderTopic();
		for ( String topic : setTopic ) {
			consumer.subscribe( topic, "*" );
		}
		consumer.setNamesrvAddr( properties.getNamesrvAddr() );
		consumer.setConsumeFromWhere( ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET );
		consumer.setConsumeMessageBatchMaxSize( 1 );
		consumer.registerMessageListener( ( List<MessageExt> msgs, ConsumeOrderlyContext context ) -> {
			MessageExt msg = msgs.get( 0 );
			try {
				publisher.publishEvent( new RocketMqEvent( msg, consumer ) );
			}
			catch ( Exception e ) {
				if ( msg.getReconsumeTimes() <= 1 ) {
					return ConsumeOrderlyStatus.SUCCESS;
				}
				else {
					System.out.println( "定时重试！" );
				}
			}
			return ConsumeOrderlyStatus.SUCCESS;
		} );
		new Thread( () -> {
			try {
				Thread.sleep( 3000 );
				try {
					consumer.start();
					System.out.println( "顺序消费开启" );
				}
				catch ( MQClientException e ) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			catch ( InterruptedException e ) {
				e.printStackTrace();
			}
		} ).start();
		return consumer;
	}

}
