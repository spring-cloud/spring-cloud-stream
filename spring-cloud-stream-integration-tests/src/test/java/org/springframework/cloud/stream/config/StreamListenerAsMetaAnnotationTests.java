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

package org.springframework.cloud.stream.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;

import static org.assertj.core.api.Assertions.assertThat;

@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@StreamListener
@interface EventHandler {

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor(annotation = StreamListener.class, attribute = "target")
	String value() default "";

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor(annotation = StreamListener.class, attribute = "target")
	String target() default "";

	/**
	 * A condition that must be met by all items that are dispatched to this method.
	 * @return a SpEL expression that must evaluate to a {@code boolean} value.
	 */
	@AliasFor(annotation = StreamListener.class, attribute = "condition")
	String condition() default "";

}

/**
 * @author David Turanski
 */
public class StreamListenerAsMetaAnnotationTests {

	@Test
	@Ignore
	public void testCustomAnnotation() {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithCustomAnnotatedArguments.class, "--server.port=0");

		TestPojoWithCustomAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithCustomAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "foo").build());
		assertThat(testPojoWithAnnotatedArguments.receivedFoo).hasSize(1);
		assertThat(testPojoWithAnnotatedArguments.receivedFoo.get(0))
				.hasFieldOrPropertyWithValue("foo", "barbar" + id);
		context.close();
	}

	@Test
	@Ignore
	public void testAnnotation() {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestPojoWithAnnotatedArguments.class, "--server.port=0");

		TestPojoWithAnnotatedArguments testPojoWithAnnotatedArguments = context
				.getBean(TestPojoWithAnnotatedArguments.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json")
				.setHeader("testHeader", "testValue").setHeader("type", "foo").build());
		assertThat(testPojoWithAnnotatedArguments.receivedFoo).hasSize(1);
		assertThat(testPojoWithAnnotatedArguments.receivedFoo.get(0))
				.hasFieldOrPropertyWithValue("foo", "barbar" + id);
		context.close();
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestPojoWithCustomAnnotatedArguments {

		List<StreamListenerTestUtils.FooPojo> receivedFoo = new ArrayList<>();

		List<StreamListenerTestUtils.BarPojo> receivedBar = new ArrayList<>();

		@EventHandler(value = Sink.INPUT, condition = "headers['type']=='foo'")
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedFoo.add(fooPojo);
		}

	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestPojoWithAnnotatedArguments {

		List<StreamListenerTestUtils.FooPojo> receivedFoo = new ArrayList<>();

		List<StreamListenerTestUtils.BarPojo> receivedBar = new ArrayList<>();

		@StreamListener(value = Sink.INPUT, condition = "headers['type']=='foo'")
		public void receive(@Payload StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedFoo.add(fooPojo);
		}

	}

}
