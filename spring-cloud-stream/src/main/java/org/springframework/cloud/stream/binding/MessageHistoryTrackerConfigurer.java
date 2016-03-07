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
package org.springframework.cloud.stream.binding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.integration.channel.ChannelInterceptorAware;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.StringUtils;

/**
 * Class that is responsible for configuring the message channel to enable message track history.
 *
 * @author Ilayaperumal Gopinathan
 */
public class MessageHistoryTrackerConfigurer implements MessageChannelConfigurer {

	public static final String HISTORY_TRACKING_HEADER = "SPRING_CLOUD_STREAM_HISTORY";

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	private final MessageBuilderFactory messageBuilderFactory;

	public MessageHistoryTrackerConfigurer(ChannelBindingServiceProperties channelBindingServiceProperties,
			MessageBuilderFactory messageBuilderFactory) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
		this.messageBuilderFactory = messageBuilderFactory;
	}

	@Override
	public void configureMessageChannel(MessageChannel messageChannel, String channelName) {
		BindingProperties bindingProperties = channelBindingServiceProperties.getBindingProperties(channelName);
		if (bindingProperties != null && Boolean.TRUE.equals(bindingProperties.isTrackHistory())) {
			final Set<String> trackHistoryProperties = StringUtils.commaDelimitedListToSet(bindingProperties.getTrackedProperties());
			Map<String, Object> channelBindingServicePropertiesMap = channelBindingServiceProperties.asMapProperties();
			final Map<String, Object> historyMap = new HashMap<>();
			if (bindingProperties.getTrackedProperties().equalsIgnoreCase("all")) {
				historyMap.putAll(channelBindingServicePropertiesMap);
			}
			else {
				for (String property : trackHistoryProperties) {
					if (channelBindingServicePropertiesMap.keySet().contains(property)) {
						historyMap.put(property, channelBindingServicePropertiesMap.get(property));
					}
				}
			}
			if (messageChannel instanceof ChannelInterceptorAware) {
				((ChannelInterceptorAware) messageChannel).addInterceptor(new ChannelInterceptorAdapter() {

					@Override
					public Message<?> preSend(Message<?> message, MessageChannel channel) {
						@SuppressWarnings("unchecked")
						Collection<Map<String, Object>> history =
								(Collection<Map<String, Object>>) message.getHeaders().get(HISTORY_TRACKING_HEADER);
						if (history == null) {
							history = new ArrayList<>(1);
						}
						else {
							history = new ArrayList<>(history);
						}
						Map<String, Object> map = new LinkedHashMap<String, Object>();
						map.put("thread", Thread.currentThread().getName());
						map.putAll(historyMap);
						history.add(map);
						Message<?> out = messageBuilderFactory
								.fromMessage(message)
								.setHeader(HISTORY_TRACKING_HEADER, history)
								.build();
						map.put("timestamp", out.getHeaders().getTimestamp());
						return out;
					}
				});
			}
		}
	}
}
