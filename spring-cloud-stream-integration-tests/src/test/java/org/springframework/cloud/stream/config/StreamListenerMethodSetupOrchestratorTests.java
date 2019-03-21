/*
 * Copyright 2018-2019 the original author or authors.
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor;
import org.springframework.cloud.stream.binding.StreamListenerSetupMethodOrchestrator;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StreamListenerMethodSetupOrchestratorTests {

	@SpyBean
	CustomOrchestrator customOrchestrator;

	@SpyBean
	MultipleStreamListenerProcessor multipleStreamListenerProcessor;

	@SpyBean
	StreamListenerAnnotationBeanPostProcessor streamListenerAnnotationBeanPostProcessor;

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomStreamListenerOrchestratorAndDefaultTogetherInSameContext()
			throws Exception {

		// Two StreamListener methods, so 2 invocations
		verify(this.customOrchestrator, times(2)).supports(any());

		Method method = this.multipleStreamListenerProcessor.getClass()
				.getMethod("handleMessage");
		StreamListener streamListener = AnnotatedElementUtils.findMergedAnnotation(method,
				StreamListener.class);
		// verify that the invocation happened on the custom Orchestrator
		verify(this.customOrchestrator).orchestrateStreamListenerSetupMethod(
				streamListener, method, this.multipleStreamListenerProcessor);

		Method method1 = this.multipleStreamListenerProcessor.getClass()
				.getMethod("produceString");
		StreamListener streamListener1 = AnnotatedElementUtils
				.findMergedAnnotation(method, StreamListener.class);

		// Verify that the invocation did not happen on the custom orchestrator
		verify(this.customOrchestrator, never()).orchestrateStreamListenerSetupMethod(
				streamListener1, method1, this.multipleStreamListenerProcessor);

		Field field = ReflectionUtils.findField(
				this.streamListenerAnnotationBeanPostProcessor.getClass(),
				"streamListenerSetupMethodOrchestrators");
		ReflectionUtils.makeAccessible(field);

		Set<StreamListenerSetupMethodOrchestrator> field1;
		field1 = (LinkedHashSet<StreamListenerSetupMethodOrchestrator>) ReflectionUtils
				.getField(field, this.streamListenerAnnotationBeanPostProcessor);
		List<StreamListenerSetupMethodOrchestrator> list = new ArrayList<>(field1);

		// Ensure that the custom orchestrator did not support this request
		assertThat(list.get(0).supports(method1)).isEqualTo(false);
		// Ensure that we are using the default Orchestrator in
		// StreamListenerAnnoatationBeanPostProcessor
		assertThat(list.get(1).supports(method1)).isEqualTo(true);
	}

	public interface SomeProcessor {

		@Input(Sink.INPUT)
		SubscribableChannel channel1();

		@Input("foobar")
		SubscribableChannel channel2();

		@Output(Source.OUTPUT)
		MessageChannel channel3();

	}

	@EnableBinding(SomeProcessor.class)
	@EnableAutoConfiguration
	public static class MultipleStreamListenerProcessor {

		@StreamListener(Sink.INPUT)
		public void handleMessage() {
		}

		@StreamListener("foobar")
		@SendTo("output")
		public String produceString() {
			return "foobar";
		}

		@Bean
		public CustomOrchestrator myOrchestrator() {
			return new CustomOrchestrator();
		}

	}

	static class CustomOrchestrator implements StreamListenerSetupMethodOrchestrator {

		@Override
		public boolean supports(Method method) {
			return method.getReturnType() != String.class;
		}

		@Override
		public void orchestrateStreamListenerSetupMethod(StreamListener streamListener,
				Method method, Object bean) {
			// stub method
		}

	}

}
