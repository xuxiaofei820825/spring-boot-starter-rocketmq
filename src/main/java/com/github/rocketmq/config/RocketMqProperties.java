package com.github.rocketmq.config;

import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@ConfigurationProperties("spring.rocketmq")
@Data
public class RocketMqProperties {

	private String namesrvAddr;

	private String instanceName;

	/** 普通消息生产者 */
	private String defaultProducer;

	/** 事务消息生产者 */
	private String transProducer;

	/** 普通消费者 */
	private String defaultConsumer;

	/** 顺序消费者 */
	private String orderConsumer;

	private Set<String> defaultTopic = new LinkedHashSet<>();
	private Set<String> orderTopic = new LinkedHashSet<>();
}
