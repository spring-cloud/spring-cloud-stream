/*
 * Copyright 2017-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binding;

import java.util.Collections;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @since 1.3
 *
 */
class MessageConverterConfigurerTests {

	@Test
	void configureOutputChannelWithBadContentType() {
		BindingServiceProperties props = new BindingServiceProperties();
		BindingProperties bindingProps = new BindingProperties();
		bindingProps.setContentType("application/json");
		props.setBindings(Collections.singletonMap("foo", bindingProps));
		CompositeMessageConverterFactory converterFactory = new CompositeMessageConverterFactory(
				Collections.<MessageConverter>emptyList(), null, null);
		MessageConverterConfigurer configurer = new MessageConverterConfigurer(props,
				converterFactory.getMessageConverterForAllRegistered());
		QueueChannel out = new QueueChannel();
		configurer.configureOutputChannel(out, "foo");
		assertThatThrownBy(() -> out.send(new GenericMessage<>(new Foo(), Collections
			.singletonMap(MessageHeaders.CONTENT_TYPE, "bad/ct")))).isInstanceOf(MessageDeliveryException.class);
	}

	@Test
	@Disabled
	void configureOutputChannelCannotConvert() {
		BindingServiceProperties props = new BindingServiceProperties();
		BindingProperties bindingProps = new BindingProperties();
		bindingProps.setContentType("foo/bar");
		props.setBindings(Collections.singletonMap("foo", bindingProps));
		MessageConverter converter = new AbstractMessageConverter(
				new MimeType("foo", "bar")) {

			@Override
			protected boolean supports(Class<?> clazz) {
				return true;
			}

			@Override
			protected Object convertToInternal(Object payload, MessageHeaders headers,
					Object conversionHint) {
				return null;
			}

		};
		CompositeMessageConverterFactory converterFactory = new CompositeMessageConverterFactory(
				Collections.<MessageConverter>singletonList(converter), null, null);
		MessageConverterConfigurer configurer = new MessageConverterConfigurer(props,
				converterFactory.getMessageConverterForAllRegistered());
		QueueChannel out = new QueueChannel();
		configurer.configureOutputChannel(out, "foo");
		assertThatThrownBy(() -> out.send(new GenericMessage<>(new Foo(),
			Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
				"bad/ct")))).isInstanceOf(MessageDeliveryException.class);
	}

	public static class Foo {

		private String bar = "bar";

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

}
