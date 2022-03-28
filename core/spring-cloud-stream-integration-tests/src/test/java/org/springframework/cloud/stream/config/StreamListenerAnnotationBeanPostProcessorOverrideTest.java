/*
 * Copyright 2016-2017 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.handler.annotation.Payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.cloud.stream.config.BindingServiceConfiguration.STREAM_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_NAME;

/**
 * @author Marius Bogoevici
 */
public class StreamListenerAnnotationBeanPostProcessorOverrideTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testOverrideStreamListenerAnnotationBeanPostProcessor() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithAnnotatedArguments.class, "--server.port=0");

		TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "foo").build());
		sink.input().send(MessageBuilder.withPayload("{\"bar\":\"foofoo" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "bar").build());
		assertThat(testPojoWithAnnotatedArguments.receivedFoo).hasSize(1);
		assertThat(testPojoWithAnnotatedArguments.receivedFoo.get(0))
				.hasFieldOrPropertyWithValue("foo", "barbar" + id);
		context.close();
	}

	@Configuration
	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments {

		List<StreamListenerTestUtils.FooPojo> receivedFoo = new ArrayList<>();

		/**
		 * Overrides the default {@link StreamListenerAnnotationBeanPostProcessor}.
		 */
		@Bean(name = STREAM_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_NAME)
		public static StreamListenerAnnotationBeanPostProcessor streamListenerAnnotationBeanPostProcessor() {
			return new StreamListenerAnnotationBeanPostProcessor() {
				@Override
				protected StreamListener postProcessAnnotation(
						StreamListener originalAnnotation, Method annotatedMethod) {
					Map<String, Object> attributes = new HashMap<>(
							AnnotationUtils.getAnnotationAttributes(originalAnnotation));
					attributes.put("condition",
							"headers['type']=='" + originalAnnotation.condition() + "'");
					return AnnotationUtils.synthesizeAnnotation(attributes,
							StreamListener.class, annotatedMethod);
				}
			};
		}

		@StreamListener(value = Sink.INPUT, condition = "foo")
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedFoo.add(fooPojo);
		}

	}

}
