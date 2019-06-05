/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.springframework.cloud.stream.converter.TestApplicationJsonMessageMarshallingConverter;
import org.springframework.core.MethodParameter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Marius Bogoevici
 * @author Ryan Dunckel
 */
public class MessageChannelToInputFluxParameterAdapterTests {

	@Test
	public void testWrapperFluxSupportsMultipleSubscriptions() throws Exception {
		List<String> results = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch latch = new CountDownLatch(4);
		final MessageChannelToInputFluxParameterAdapter messageChannelToInputFluxParameterAdapter;
		messageChannelToInputFluxParameterAdapter = new MessageChannelToInputFluxParameterAdapter(
				new CompositeMessageConverter(
						Collections.singleton(new MappingJackson2MessageConverter())));
		final Method processMethod = ReflectionUtils.findMethod(
				MessageChannelToInputFluxParameterAdapterTests.class, "process",
				Flux.class);
		final DirectChannel adaptedChannel = new DirectChannel();
		@SuppressWarnings("unchecked")
		final Flux<Message<?>> adapterFlux = (Flux<Message<?>>) messageChannelToInputFluxParameterAdapter
				.adapt(adaptedChannel, new MethodParameter(processMethod, 0));
		String uuid1 = UUID.randomUUID().toString();
		String uuid2 = UUID.randomUUID().toString();
		adapterFlux.map(m -> m.getPayload() + uuid1).subscribe(s -> {
			results.add(s);
			latch.countDown();
		});
		adapterFlux.map(m -> m.getPayload() + uuid2).subscribe(s -> {
			results.add(s);
			latch.countDown();
		});

		adaptedChannel.send(MessageBuilder.withPayload("A").build());
		adaptedChannel.send(MessageBuilder.withPayload("B").build());

		assertThat(latch.await(5000, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(results).containsExactlyInAnyOrder("A" + uuid1, "B" + uuid1,
				"A" + uuid2, "B" + uuid2);

	}

	@Test
	public void testAdapterConvertsUsingConversionHint() {
		CompositeMessageConverter messageConverter = new CompositeMessageConverter(
				Collections
						.singleton(new TestApplicationJsonMessageMarshallingConverter()));

		MessageChannelToInputFluxParameterAdapter adapter = new MessageChannelToInputFluxParameterAdapter(
				messageConverter);

		Method processMethod = ReflectionUtils.findMethod(
				MessageChannelToInputFluxParameterAdapterTests.class, "processNestedGenericFlux",
				Flux.class);

		DirectChannel adaptedChannel = new DirectChannel();

		@SuppressWarnings("unchecked")
		Flux<FirstLevelWrapper<SecondLevelWrapper<String>>> adapterFlux = (Flux<FirstLevelWrapper<SecondLevelWrapper<String>>>) adapter
				.adapt(adaptedChannel, new MethodParameter(processMethod, 0));

		SecondLevelWrapper<String> expected2 = new SecondLevelWrapper<>();
		expected2.setName("name");
		expected2.setData("data");

		FirstLevelWrapper<SecondLevelWrapper<String>> expected1 = new FirstLevelWrapper<>();
		expected1.setId(1);
		expected1.setData(expected2);

		StepVerifier.create(adapterFlux).then(() -> {
			adaptedChannel.send(MessageBuilder.withPayload(
					"{ \"id\": 1, \"data\": { \"name\": \"name\", \"data\": \"data\" } }")
					.build());
		}).expectNext(expected1).thenCancel().verify();
	}

	public void process(Flux<Message<?>> message) {
		// do nothing - we just reference this method from the test
	}

	public void processNestedGenericFlux(Flux<FirstLevelWrapper<SecondLevelWrapper<String>>> items) {
		// do nothing - we just reference this method from the test
	}

	static class FirstLevelWrapper<T> {

		private long id;

		private T data;

		long getId() {
			return id;
		}

		void setId(long id) {
			this.id = id;
		}

		T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}

		@Override
		public int hashCode() {
			return Objects.hash(data, id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (!(obj instanceof FirstLevelWrapper)) {
				return false;
			}

			@SuppressWarnings("unchecked")
			FirstLevelWrapper<T> other = (FirstLevelWrapper<T>) obj;
			return Objects.equals(data, other.data) && id == other.id;
		}

		@Override
		public String toString() {
			return "FirstLevelWrapper [id=" + id + ", data=" + data + "]";
		}

	}

	static class SecondLevelWrapper<T> {

		private String name;

		private T data;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}

		@Override
		public int hashCode() {
			return Objects.hash(data, name);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (!(obj instanceof SecondLevelWrapper)) {
				return false;
			}

			@SuppressWarnings("unchecked")
			SecondLevelWrapper<T> other = (SecondLevelWrapper<T>) obj;
			return Objects.equals(data, other.data) && Objects.equals(name, other.name);
		}

		@Override
		public String toString() {
			return "SecondLevelWrapper [name=" + name + ", data=" + data + "]";
		}

	}

}
