/*
 * Copyright 2016 the original author or authors.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.Assert;

/**
 * @author Ilayaperumal Gopinathan
 */
public class MessageHistoryTrackerConfigurerTests {

	@Test
	public void testHistoryTrackAll() {
		ChannelBindingServiceProperties serviceProperties = new ChannelBindingServiceProperties();
		serviceProperties.setInstanceCount(2);
		serviceProperties.setInstanceIndex(0);
		Map<String, BindingProperties> bindingPropertiesMap = new HashMap<>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setTrackHistory(true);
		bindingPropertiesMap.put("input", bindingProperties);
		bindingPropertiesMap.put("test1", new BindingProperties());
		serviceProperties.setBindings(bindingPropertiesMap);
		MessageHistoryTrackerConfigurer historyTrackerConfigurer = new MessageHistoryTrackerConfigurer(serviceProperties,
				new MutableMessageBuilderFactory());
		DirectChannel messageChannel = new DirectChannel();
		messageChannel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(message.getHeaders().containsKey(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER));
				List<Map<String, Object>> headerValues =  (List<Map<String, Object>>) message.getHeaders().get(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER);
				Map<String, Object> historyValues = headerValues.get(0);
				Assert.isTrue(historyValues.containsKey("instanceIndex") && historyValues.get("instanceIndex").equals("0"), "Instance index must exist with value '0'");
				Assert.isTrue(historyValues.containsKey("instanceCount") && historyValues.get("instanceCount").equals("2"), "Instance count must exist with value '2'");
				Assert.isTrue(historyValues.containsKey("input"), "Binding properties must exist for the channel 'input'");
				Assert.isTrue(historyValues.containsKey("test1"), "Binding properties must exist for the channel 'test1'");
			}
		});
		historyTrackerConfigurer.configureMessageChannel(messageChannel, "input");
		messageChannel.send(MessageBuilder.withPayload("test").build());
	}

	@Test
	public void testHistoryTrackSpecificProperties() {
		ChannelBindingServiceProperties serviceProperties = new ChannelBindingServiceProperties();
		serviceProperties.setInstanceCount(2);
		serviceProperties.setInstanceIndex(0);
		Map<String, BindingProperties> bindingPropertiesMap = new HashMap<>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setTrackHistory(true);
		bindingProperties.setTrackedProperties("input,instanceIndex");
		bindingPropertiesMap.put("input", bindingProperties);
		bindingPropertiesMap.put("test1", new BindingProperties());
		serviceProperties.setBindings(bindingPropertiesMap);
		MessageHistoryTrackerConfigurer historyTrackerConfigurer = new MessageHistoryTrackerConfigurer(serviceProperties,
				new MutableMessageBuilderFactory());
		DirectChannel messageChannel = new DirectChannel();
		messageChannel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(message.getHeaders().containsKey(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER));
				List<Map<String, Object>> headerValues =  (List<Map<String, Object>>) message.getHeaders().get(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER);
				Map<String, Object> historyValues = headerValues.get(0);
				Assert.isTrue(historyValues.containsKey("thread"), "Default property 'thread' should exist.");
				Assert.isTrue(historyValues.containsKey("timestamp"), "Default property 'timestamp' should exist.");
				Assert.isTrue(historyValues.containsKey("instanceIndex") && historyValues.get("instanceIndex").equals("0"), "Instance index must exist with value '0'");
				Assert.isTrue(!historyValues.containsKey("instanceCount"), "Instance count should not be in the tracker header");
				Assert.isTrue(historyValues.containsKey("input"), "Binding properties must exist for the channel 'input'");
				Assert.isTrue(!historyValues.containsKey("test1"), "Binding properties for the channel 'test1' should not be in the tracker header");
			}
		});
		historyTrackerConfigurer.configureMessageChannel(messageChannel, "input");
		messageChannel.send(MessageBuilder.withPayload("test").build());
	}

	@Test
	public void testHistoryTrackEmptyProperties() {
		ChannelBindingServiceProperties serviceProperties = new ChannelBindingServiceProperties();
		serviceProperties.setInstanceCount(2);
		serviceProperties.setInstanceIndex(0);
		Map<String, BindingProperties> bindingPropertiesMap = new HashMap<>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setTrackHistory(true);
		bindingProperties.setTrackedProperties("");
		bindingPropertiesMap.put("input", bindingProperties);
		bindingPropertiesMap.put("test1", new BindingProperties());
		serviceProperties.setBindings(bindingPropertiesMap);
		MessageHistoryTrackerConfigurer historyTrackerConfigurer = new MessageHistoryTrackerConfigurer(serviceProperties,
				new MutableMessageBuilderFactory());
		DirectChannel messageChannel = new DirectChannel();
		messageChannel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				Assert.isTrue(message.getHeaders().containsKey(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER));
				List<Map<String, Object>> headerValues =  (List<Map<String, Object>>) message.getHeaders().get(MessageHistoryTrackerConfigurer.HISTORY_TRACKING_HEADER);
				Map<String, Object> historyValues = headerValues.get(0);
				Assert.isTrue(historyValues.containsKey("thread"), "Default property 'thread' should exist.");
				Assert.isTrue(historyValues.containsKey("timestamp"), "Default property 'timestamp' should exist.");
			}
		});
		historyTrackerConfigurer.configureMessageChannel(messageChannel, "input");
		messageChannel.send(MessageBuilder.withPayload("test").build());
	}
}
