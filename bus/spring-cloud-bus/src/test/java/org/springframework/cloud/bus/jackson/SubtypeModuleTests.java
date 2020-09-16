/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus.jackson;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.junit.Test;

import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.UnknownRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TypedRemoteApplicationEvent;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Spencer Gibb
 * @author Stefan Pfeiffer
 */
public class SubtypeModuleTests {

	@Test
	public void testDeserializeSubclass() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new SubtypeModule(MyRemoteApplicationEvent.class));

		RemoteApplicationEvent event = mapper.readValue(
				"{\"type\":\"my\", \"destinationService\":\"myservice\", \"originService\":\"myorigin\"}",
				RemoteApplicationEvent.class);
		assertThat(event instanceof MyRemoteApplicationEvent).as("event is wrong type").isTrue();
		MyRemoteApplicationEvent myEvent = MyRemoteApplicationEvent.class.cast(event);
		assertThat(myEvent.getOriginService()).as("originService was wrong").isEqualTo("myorigin");
		assertThat(myEvent.getDestinationService()).as("destinationService was wrong").isEqualTo("myservice");
	}

	@Test
	public void testDeserializeWhenTypeIsKnown() throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		RemoteApplicationEvent event = mapper.readValue("{\"type\":\"another\"}", AnotherRemoteApplicationEvent.class);
		assertThat(event instanceof AnotherRemoteApplicationEvent).as("event is wrong type").isTrue();
	}

	@Test
	public void testDeserializeCustomizedObjectMapper() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(mapper);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(MessageBuilder
				.withPayload("{\"type\":\"TestRemoteApplicationEvent\", \"origin_service\":\"myorigin\"}").build(),
				RemoteApplicationEvent.class);
		assertThat(event).isNotNull().isInstanceOf(TestRemoteApplicationEvent.class);
		assertThat(TestRemoteApplicationEvent.class.cast(event).getOriginService()).isEqualTo("myorigin");
	}

	@Test
	public void testDeserializeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"TestRemoteApplicationEvent\"}").build(),
				RemoteApplicationEvent.class);
		assertThat(event instanceof TestRemoteApplicationEvent).as("event is wrong type").isTrue();
	}

	@Test
	public void testDeserializeUnknownTypeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"NotDefinedTestRemoteApplicationEvent\"}").build(),
				RemoteApplicationEvent.class);
		assertThat(event instanceof UnknownRemoteApplicationEvent).as("event is wrong type").isTrue();
		assertThat(((UnknownRemoteApplicationEvent) event).getTypeInfo()).as("type information is wrong")
				.isEqualTo("NotDefinedTestRemoteApplicationEvent");
		assertThat(((UnknownRemoteApplicationEvent) event).getPayloadAsString()).as("payload is wrong")
				.isEqualTo("{\"type\":\"NotDefinedTestRemoteApplicationEvent\"}");
	}

	@Test
	public void testDeserializeJsonTypeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(MessageBuilder.withPayload("{\"type\":\"typed\"}").build(),
				RemoteApplicationEvent.class);
		assertThat(event instanceof TypedRemoteApplicationEvent).as("event is wrong type").isTrue();
	}

	/**
	 * see https://github.com/spring-cloud/spring-cloud-bus/issues/74
	 */
	@Test
	public void testDeserializeAckRemoteApplicationEventWithKnownType() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.afterPropertiesSet();
		Object event = converter
				.fromMessage(MessageBuilder
						.withPayload("{\"type\":\"AckRemoteApplicationEvent\", "
								+ "\"event\":\"org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent\"}")
						.build(), RemoteApplicationEvent.class);
		assertThat(event instanceof AckRemoteApplicationEvent).as("event is no ack").isTrue();
		AckRemoteApplicationEvent ackEvent = AckRemoteApplicationEvent.class.cast(event);
		assertThat(ackEvent.getEvent()).as("inner ack event has wrong type")
				.isEqualTo(TestRemoteApplicationEvent.class);
	}

	/**
	 * see https://github.com/spring-cloud/spring-cloud-bus/issues/74
	 */
	@Test
	public void testDeserializeAckRemoteApplicationEventWithUnknownType() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(MessageBuilder
				.withPayload(
						"{\"type\":\"AckRemoteApplicationEvent\", \"event\":\"foo.bar.TestRemoteApplicationEvent\"}")
				.build(), RemoteApplicationEvent.class);
		assertThat(event instanceof AckRemoteApplicationEvent).as("event is no ack").isTrue();
		AckRemoteApplicationEvent ackEvent = AckRemoteApplicationEvent.class.cast(event);
		assertThat(ackEvent.getEvent()).as("inner ack event has wrong type")
				.isEqualTo(UnknownRemoteApplicationEvent.class);
	}

	@SuppressWarnings("serial")
	@JsonTypeName("my")
	public static class MyRemoteApplicationEvent extends RemoteApplicationEvent {

		@SuppressWarnings("unused")
		private MyRemoteApplicationEvent() {
		}

		protected MyRemoteApplicationEvent(Object source, String originService, String destinationService) {
			super(source, originService, destinationService);
		}

		protected MyRemoteApplicationEvent(Object source, String originService) {
			super(source, originService);
		}

	}

	@SuppressWarnings("serial")
	@JsonTypeName("another")
	public static class AnotherRemoteApplicationEvent extends RemoteApplicationEvent {

		@SuppressWarnings("unused")
		private AnotherRemoteApplicationEvent() {
		}

		protected AnotherRemoteApplicationEvent(Object source, String originService, String destinationService) {
			super(source, originService, destinationService);
		}

		protected AnotherRemoteApplicationEvent(Object source, String originService) {
			super(source, originService);
		}

	}

}
