/*
 * Copyright 2015 the original author or authors.
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

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.AbstractFromMessageConverter;
import org.springframework.cloud.stream.converter.ByteArrayToStringMessageConverter;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.JavaToSerializedMessageConverter;
import org.springframework.cloud.stream.converter.JsonToPojoMessageConverter;
import org.springframework.cloud.stream.converter.JsonToTupleMessageConverter;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.converter.PojoToJsonMessageConverter;
import org.springframework.cloud.stream.converter.PojoToStringMessageConverter;
import org.springframework.cloud.stream.converter.SerializedToJavaMessageConverter;
import org.springframework.cloud.stream.converter.StringToByteArrayMessageConverter;
import org.springframework.cloud.stream.converter.TupleToJsonMessageConverter;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 *
 * @author Ilayaperumal Gopinathan
 */
public class MessageConverterConfigurer implements MessageChannelConfigurer, BeanFactoryAware, InitializingBean {

	private ConfigurableListableBeanFactory beanFactory;

	private CompositeMessageConverterFactory messageConverterFactory;

	private final ChannelBindingServiceProperties channelBindingServiceProperties;

	public MessageConverterConfigurer(ChannelBindingServiceProperties channelBindingServiceProperties) {
		this.channelBindingServiceProperties = channelBindingServiceProperties;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.beanFactory, "Bean factory cannot be empty");
		Set<AbstractFromMessageConverter> messageConverters = new HashSet<>();
		messageConverters.add(new JsonToTupleMessageConverter());
		messageConverters.add(new TupleToJsonMessageConverter());
		messageConverters.add(new JsonToPojoMessageConverter());
		messageConverters.add(new PojoToJsonMessageConverter());
		messageConverters.add(new ByteArrayToStringMessageConverter());
		messageConverters.add(new StringToByteArrayMessageConverter());
		messageConverters.add(new PojoToStringMessageConverter());
		messageConverters.add(new JavaToSerializedMessageConverter());
		messageConverters.add(new SerializedToJavaMessageConverter());
		this.messageConverterFactory = new CompositeMessageConverterFactory(messageConverters);
	}

	/**
	 * Setup data-type and message converters for the given message channel.
	 *
	 * @param channel message channel to set the data-type and message converters
	 * @param channelName the channel name
	 */
	@Override
	public void configureMessageChannel(MessageChannel channel, String channelName) {
		Assert.isAssignable(AbstractMessageChannel.class, channel.getClass());
		AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
		BindingProperties bindingProperties = this.channelBindingServiceProperties.getBindingProperties(channelName);
		if (bindingProperties != null) {
			String contentType = bindingProperties.getContentType();
			if (StringUtils.hasText(contentType)) {
				MimeType mimeType = MessageConverterUtils.getMimeType(contentType);
				MessageConverter messageConverter = this.messageConverterFactory.newInstance(mimeType);
				Class<?> dataType = MessageConverterUtils.getJavaTypeForContentType(mimeType,
						Thread.currentThread().getContextClassLoader());
				messageChannel.setDatatypes(dataType);
				messageChannel.setMessageConverter(messageConverter);
			}
		}
	}
}
