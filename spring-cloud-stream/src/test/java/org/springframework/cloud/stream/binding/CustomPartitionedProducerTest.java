/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.partitioning.CustomPartitionKeyExtractorClass;
import org.springframework.cloud.stream.partitioning.CustomPartitionSelectorClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.util.ReflectionUtils;

/**
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class CustomPartitionedProducerTest {

	@Test
	public void testCustomPartitionedProducer() {
		ApplicationContext context = SpringApplication.run(CustomPartitionedProducerTest.TestSource.class,
				"--spring.jmx.enabled=false",
				"--spring.main.web-application-type=none",
				"--spring.cloud.stream.bindings.output.producer.partitionKeyExtractorClass=org.springframework.cloud.stream.partitioning.CustomPartitionKeyExtractorClass",
				"--spring.cloud.stream.bindings.output.producer.partitionSelectorClass=org.springframework.cloud.stream.partitioning.CustomPartitionSelectorClass",
				"--spring.cloud.stream.default-binder=mock");
		Source testSource = context.getBean(Source.class);
		DirectChannel messageChannel = (DirectChannel) testSource.output();
		for (ChannelInterceptor channelInterceptor : messageChannel.getChannelInterceptors()) {
			if (channelInterceptor instanceof MessageConverterConfigurer.PartitioningInterceptor) {
				Field partitionHandlerField = ReflectionUtils
						.findField(MessageConverterConfigurer.PartitioningInterceptor.class, "partitionHandler");
				ReflectionUtils.makeAccessible(partitionHandlerField);
				PartitionHandler partitionHandler = (PartitionHandler) ReflectionUtils.getField(partitionHandlerField,
						channelInterceptor);
				Field partitonKeyExtractorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionKeyExtractorStrategy");
				ReflectionUtils.makeAccessible(partitonKeyExtractorField);
				Field partitonSelectorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionSelectorStrategy");
				ReflectionUtils.makeAccessible(partitonSelectorField);
				Assert.assertTrue(((PartitionKeyExtractorStrategy) ReflectionUtils.getField(partitonKeyExtractorField,
						partitionHandler)).getClass().equals(CustomPartitionKeyExtractorClass.class));
				Assert.assertTrue(
						((PartitionSelectorStrategy) ReflectionUtils.getField(partitonSelectorField, partitionHandler))
								.getClass().equals(CustomPartitionSelectorClass.class));
			}
		}
	}

	@Test
	public void testCustomPartitionedProducerByName() {
		ApplicationContext context = SpringApplication.run(CustomPartitionedProducerTest.TestSource.class,
				"--spring.jmx.enabled=false",
				"--spring.main.web-application-type=none",
				"--spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName=customPartitionKeyExtractor",
				"--spring.cloud.stream.bindings.output.producer.partitionSelectorName=customPartitionSelector",
				"--spring.cloud.stream.default-binder=mock");
		Source testSource = context.getBean(Source.class);
		DirectChannel messageChannel = (DirectChannel) testSource.output();
		for (ChannelInterceptor channelInterceptor : messageChannel.getChannelInterceptors()) {
			if (channelInterceptor instanceof MessageConverterConfigurer.PartitioningInterceptor) {
				Field partitionHandlerField = ReflectionUtils
						.findField(MessageConverterConfigurer.PartitioningInterceptor.class, "partitionHandler");
				ReflectionUtils.makeAccessible(partitionHandlerField);
				PartitionHandler partitionHandler = (PartitionHandler) ReflectionUtils.getField(partitionHandlerField,
						channelInterceptor);
				Field partitonKeyExtractorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionKeyExtractorStrategy");
				ReflectionUtils.makeAccessible(partitonKeyExtractorField);
				Field partitonSelectorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionSelectorStrategy");
				ReflectionUtils.makeAccessible(partitonSelectorField);
				Assert.assertTrue(((PartitionKeyExtractorStrategy) ReflectionUtils.getField(partitonKeyExtractorField,
						partitionHandler)).getClass().equals(CustomPartitionKeyExtractorClass.class));
				Assert.assertTrue(
						((PartitionSelectorStrategy) ReflectionUtils.getField(partitonSelectorField, partitionHandler))
								.getClass().equals(CustomPartitionSelectorClass.class));
			}
		}
	}

	@Test
	public void testCustomPartitionedProducerAsSingletons() {
		ApplicationContext context = SpringApplication.run(CustomPartitionedProducerTest.TestSource.class,
				"--spring.jmx.enabled=false", "--spring.main.web-application-type=none",
				"--spring.cloud.stream.default-binder=mock");
		Source testSource = context.getBean(Source.class);
		DirectChannel messageChannel = (DirectChannel) testSource.output();
		for (ChannelInterceptor channelInterceptor : messageChannel.getChannelInterceptors()) {
			if (channelInterceptor instanceof MessageConverterConfigurer.PartitioningInterceptor) {
				Field partitionHandlerField = ReflectionUtils
						.findField(MessageConverterConfigurer.PartitioningInterceptor.class, "partitionHandler");
				ReflectionUtils.makeAccessible(partitionHandlerField);
				PartitionHandler partitionHandler = (PartitionHandler) ReflectionUtils.getField(partitionHandlerField,
						channelInterceptor);
				Field partitonKeyExtractorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionKeyExtractorStrategy");
				ReflectionUtils.makeAccessible(partitonKeyExtractorField);
				Field partitonSelectorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionSelectorStrategy");
				ReflectionUtils.makeAccessible(partitonSelectorField);
				Assert.assertTrue(((PartitionKeyExtractorStrategy) ReflectionUtils.getField(partitonKeyExtractorField,
						partitionHandler)).getClass().equals(CustomPartitionKeyExtractorClass.class));
				Assert.assertTrue(
						((PartitionSelectorStrategy) ReflectionUtils.getField(partitonSelectorField, partitionHandler))
								.getClass().equals(CustomPartitionSelectorClass.class));
			}
		}
	}

	public void testCustomPartitionedProducerMultipleInstances() {
		ApplicationContext context = SpringApplication.run(CustomPartitionedProducerTest.TestSourceMultipleStrategies.class,
				"--spring.jmx.enabled=false",
				"--spring.main.web-application-type=none",
				"--spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName=customPartitionKeyExtractorOne",
				"--spring.cloud.stream.bindings.output.producer.partitionSelectorName=customPartitionSelectorTwo",
				"--spring.cloud.stream.default-binder=mock");
		Source testSource = context.getBean(Source.class);
		DirectChannel messageChannel = (DirectChannel) testSource.output();
		for (ChannelInterceptor channelInterceptor : messageChannel.getChannelInterceptors()) {
			if (channelInterceptor instanceof MessageConverterConfigurer.PartitioningInterceptor) {
				Field partitionHandlerField = ReflectionUtils
						.findField(MessageConverterConfigurer.PartitioningInterceptor.class, "partitionHandler");
				ReflectionUtils.makeAccessible(partitionHandlerField);
				PartitionHandler partitionHandler = (PartitionHandler) ReflectionUtils.getField(partitionHandlerField,
						channelInterceptor);
				Field partitonKeyExtractorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionKeyExtractorStrategy");
				ReflectionUtils.makeAccessible(partitonKeyExtractorField);
				Field partitonSelectorField = ReflectionUtils.findField(PartitionHandler.class,
						"partitionSelectorStrategy");
				ReflectionUtils.makeAccessible(partitonSelectorField);
				Assert.assertTrue(((PartitionKeyExtractorStrategy) ReflectionUtils.getField(partitonKeyExtractorField,
						partitionHandler)).getClass().equals(CustomPartitionKeyExtractorClass.class));
				Assert.assertTrue(
						((PartitionSelectorStrategy) ReflectionUtils.getField(partitonSelectorField, partitionHandler))
								.getClass().equals(CustomPartitionSelectorClass.class));
			}
		}
	}

	@Test(expected=Exception.class)
	// It actually throws UnsatisfiedDependencyException, but it is confusing when it comes to test
	// But for the purposes of the test all we care about is that it fails
	public void testCustomPartitionedProducerMultipleInstancesFailNoFilter() {
		SpringApplication.run(CustomPartitionedProducerTest.TestSourceMultipleStrategies.class,
				"--spring.jmx.enabled=false", "--spring.main.web-application-type=none");
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/custom-partitioned-producer-test.properties")
	public static class TestSource {

		@Bean
		public CustomPartitionSelectorClass customPartitionSelector() {
			return new CustomPartitionSelectorClass();
		}

		@Bean
		public CustomPartitionKeyExtractorClass customPartitionKeyExtractor() {
			return new CustomPartitionKeyExtractorClass();
		}

		@Bean
		@InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "5000", maxMessagesPerPoll = "1"))
		public MessageSource<String> timerMessageSource() {
			return new MessageSource<String>() {
				@Override
				public Message<String> receive() {
					throw new MessagingException("test");
				}
			};
		}
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/custom-partitioned-producer-test.properties")
	public static class TestSourceMultipleStrategies {

		@Bean
		public CustomPartitionSelectorClass customPartitionSelectorOne() {
			return new CustomPartitionSelectorClass();
		}

		@Bean
		public CustomPartitionSelectorClass customPartitionSelectorTwo() {
			return new CustomPartitionSelectorClass();
		}

		@Bean
		public CustomPartitionKeyExtractorClass customPartitionKeyExtractorOne() {
			return new CustomPartitionKeyExtractorClass();
		}

		@Bean
		public CustomPartitionKeyExtractorClass customPartitionKeyExtractorTwo() {
			return new CustomPartitionKeyExtractorClass();
		}

		@Bean
		@InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "5000", maxMessagesPerPoll = "1"))
		public MessageSource<String> timerMessageSource() {
			return new MessageSource<String>() {
				@Override
				public Message<String> receive() {
					throw new MessagingException("test");
				}
			};
		}
	}
}
