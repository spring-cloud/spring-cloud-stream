/*
 * Copyright 2017-2019 the original author or authors.
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

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @since 1.3
 *
 */
public class MessageConverterConfigurerTests {

	// @Test
	public void testConfigureOutputChannelWithBadContentType() {
		BindingServiceProperties props = new BindingServiceProperties();
		BindingProperties bindingProps = new BindingProperties();
		bindingProps.setContentType("application/json");
		props.setBindings(Collections.singletonMap("foo", bindingProps));
		CompositeMessageConverterFactory converterFactory = new CompositeMessageConverterFactory(
				Collections.<MessageConverter>emptyList(), null);
		MessageConverterConfigurer configurer = new MessageConverterConfigurer(props,
				converterFactory.getMessageConverterForAllRegistered());
		QueueChannel out = new QueueChannel();
		configurer.configureOutputChannel(out, "foo");
		out.send(new GenericMessage<Foo>(new Foo(), Collections
				.<String, Object>singletonMap(MessageHeaders.CONTENT_TYPE, "bad/ct")));
		Message<?> received = out.receive(0);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isInstanceOf(Foo.class);
	}

	@Test
	@Ignore
	public void testConfigureOutputChannelCannotConvert() {
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
				Collections.<MessageConverter>singletonList(converter), null);
		MessageConverterConfigurer configurer = new MessageConverterConfigurer(props,
				converterFactory.getMessageConverterForAllRegistered());
		QueueChannel out = new QueueChannel();
		configurer.configureOutputChannel(out, "foo");
		try {
			out.send(new GenericMessage<Foo>(new Foo(),
					Collections.<String, Object>singletonMap(MessageHeaders.CONTENT_TYPE,
							"bad/ct")));
			fail("Expected MessageConversionException: " + out.receive(0));
		}
		catch (MessageConversionException e) {
			assertThat(e.getMessage())
					.endsWith("to the configured output type: 'foo/bar'");
		}
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
