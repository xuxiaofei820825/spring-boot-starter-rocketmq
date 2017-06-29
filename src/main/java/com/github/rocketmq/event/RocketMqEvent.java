package com.github.rocketmq.event;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.ApplicationEvent;

import lombok.Data;
import lombok.EqualsAndHashCode;

@SuppressWarnings("serial")
@Data
@EqualsAndHashCode(callSuper = false)
public class RocketMqEvent extends ApplicationEvent {

	private DefaultMQPushConsumer consumer;
	private MessageExt messageExt;
	private String topic;
	private String tag;
	private byte[] body;

	public RocketMqEvent( Object source ) {
		super( source );
	}

	public RocketMqEvent( MessageExt msg, DefaultMQPushConsumer consumer ) {
		super( msg );

		this.topic = msg.getTopic();
		this.tag = msg.getTags();
		this.body = msg.getBody();
		this.consumer = consumer;
		this.messageExt = msg;
	}

	public String getMsg() {
		try {
			return new String( this.body, "utf-8" );
		}
		catch ( UnsupportedEncodingException e ) {
			return null;
		}
	}

	public String getMsg( String code ) {
		try {
			return new String( this.body, code );
		}
		catch ( UnsupportedEncodingException e ) {
			return null;
		}
	}
}
