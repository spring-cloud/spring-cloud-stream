/*
 * Copyright 2017 the original author or authors.
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
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.partitioning.CustomPartitionKeyExtractorClass;
import org.springframework.cloud.stream.partitioning.CustomPartitionSelectorClass;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = CustomPartitionedProducerTest.TestSource.class)
public class CustomPartitionedProducerTest {

	@Autowired
	private Source testSource;

	@Test
	public void testCustomPartitionedProducer() {
		DirectChannel messageChannel = (DirectChannel) this.testSource.output();
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

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/custom-partitioned-producer-test.properties")
	public static class TestSource {

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
