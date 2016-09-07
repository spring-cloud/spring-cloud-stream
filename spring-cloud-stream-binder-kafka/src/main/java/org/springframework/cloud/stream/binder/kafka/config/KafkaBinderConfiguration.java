/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.config;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka09AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka10AdminUtilsOperation;
import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.integration.codec.Codec;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({KryoCodecAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class})
@EnableConfigurationProperties({KafkaBinderConfigurationProperties.class, KafkaExtendedBindingProperties.class})
public class KafkaBinderConfiguration {

	protected final Log logger = LogFactory.getLog(getClass());

	@Autowired
	private Codec codec;

	@Autowired
	private KafkaBinderConfigurationProperties configurationProperties;

	@Autowired
	private KafkaExtendedBindingProperties kafkaExtendedBindingProperties;

	@Autowired
	private ProducerListener producerListener;

	@Autowired
	private ApplicationContext context;

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder() {
		KafkaMessageChannelBinder kafkaMessageChannelBinder = new KafkaMessageChannelBinder(
				this.configurationProperties);
		kafkaMessageChannelBinder.setCodec(this.codec);
		//kafkaMessageChannelBinder.setProducerListener(producerListener);
		kafkaMessageChannelBinder.setExtendedBindingProperties(this.kafkaExtendedBindingProperties);
		AdminUtilsOperation adminUtilsOperation = context.getBean(AdminUtilsOperation.class);
		kafkaMessageChannelBinder.setAdminUtilsOperation(adminUtilsOperation);
		return kafkaMessageChannelBinder;
	}

	@Bean
	@ConditionalOnMissingBean(ProducerListener.class)
	ProducerListener producerListener() {
		return new LoggingProducerListener();
	}

	@Bean
	KafkaBinderHealthIndicator healthIndicator(KafkaMessageChannelBinder kafkaMessageChannelBinder) {
		return new KafkaBinderHealthIndicator(kafkaMessageChannelBinder, this.configurationProperties);
	}

	@Bean(name = "adminUtilsOperation")
	@Conditional(Kafka09Condition.class)
	public AdminUtilsOperation kafka09AdminUtilsOperation() {
		logger.info("AdminUtils selected: Kafka 0.9 AdminUtils");
		return new Kafka09AdminUtilsOperation();
	}

	@Bean(name = "adminUtilsOperation")
	@Conditional(Kafka10Condition.class)
	public AdminUtilsOperation kafka10AdminUtilsOperation() {
		logger.info("AdminUtils selected: Kafka 0.10 AdminUtils");
		return new Kafka10AdminUtilsOperation();
	}

	private static Method getMethod(ClassLoader classLoader, String methodName) {
		try {
			Class<?> adminUtilClass = classLoader.loadClass("kafka.admin.AdminUtils");
			Method[] declaredMethods = adminUtilClass.getDeclaredMethods();
			for (Method m : declaredMethods) {
				if (m.getName().equals(methodName)) {
					return m;
				}
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("AdminUtils not found", e);
		}
		return null;
	}

	static class Kafka10Condition implements Condition {

		@Override
		public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
			ClassLoader classLoader = Kafka10Condition.class.getClassLoader();
			Method addPartitions = getMethod(classLoader, "addPartitions");
			if (addPartitions != null) {
				Class<?>[] parameterTypes = addPartitions.getParameterTypes();
				Class clazz = parameterTypes[parameterTypes.length - 1];
				if (clazz.getName().equals("kafka.admin.RackAwareMode")) {
					return true;
				}
			}
			return false;
		}
	}

	static class Kafka09Condition implements Condition {

		@Override
		public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {

			ClassLoader classLoader = Kafka09Condition.class.getClassLoader();
			Method addPartitions = getMethod(classLoader, "addPartitions");
			if (addPartitions != null) {
				Class<?>[] parameterTypes = addPartitions.getParameterTypes();
				Class clazz = parameterTypes[parameterTypes.length - 1];
				if (!clazz.getName().equals("kafka.admin.RackAwareMode")) {
					return true;
				}
			}
			return false;
		}
	}
}
