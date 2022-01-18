/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = CustomMessageConverterTests.TestSource.class)
public class CustomMessageConverterTests {

	@Autowired
	private Source testSource;

	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	private List<MessageConverter> customMessageConverters;

	@Test
	public void testCustomMessageConverter() throws Exception {
		assertThat(this.customMessageConverters).extracting("class")
				.contains(FooConverter.class, BarConverter.class);
		this.testSource.output().send(MessageBuilder.withPayload(new Foo("hi")).build());
		@SuppressWarnings("unchecked")
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testSource.output()).poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeType.valueOf("test/foo"));
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/custom/source-channel-configurers.properties")
	@Configuration
	public static class TestSource {

		@Bean
		public MessageConverter fooConverter() {
			return new FooConverter();
		}

		@Bean
		public MessageConverter barConverter() {
			return new BarConverter();
		}

	}

	public static class FooConverter extends AbstractMessageConverter {

		public FooConverter() {
			super(MimeType.valueOf("test/foo"));
		}

		@Override
		protected boolean supports(Class<?> clazz) {
			return clazz.equals(Foo.class);
		}

		@Override
		protected Object convertToInternal(Object payload, MessageHeaders headers,
				Object conversionHint) {
			Object result = null;
			try {
				if (payload instanceof Foo) {
					Foo fooPayload = (Foo) payload;
					result = fooPayload.test.getBytes();
				}
			}
			catch (Exception e) {
				this.logger.error(e.getMessage(), e);
				return null;
			}
			return result;
		}

	}

	public static class BarConverter extends AbstractMessageConverter {

		public BarConverter() {
			super(MimeType.valueOf("test/bar"));
		}

		@Override
		protected boolean supports(Class<?> clazz) {
			return clazz.equals(Bar.class);
		}

		@Override
		protected Object convertToInternal(Object payload, MessageHeaders headers,
				Object conversionHint) {
			Object result = null;
			try {
				if (payload instanceof Bar) {
					Bar barPayload = (Bar) payload;
					result = barPayload.testing.getBytes();
				}
			}
			catch (Exception e) {
				this.logger.error(e.getMessage(), e);
				return null;
			}
			return result;
		}

	}

	public static class Foo {

		final String test;

		public Foo(String test) {
			this.test = test;
		}

	}

	public static class Bar {

		final String testing;

		public Bar(String testing) {
			this.testing = testing;
		}

	}

}
