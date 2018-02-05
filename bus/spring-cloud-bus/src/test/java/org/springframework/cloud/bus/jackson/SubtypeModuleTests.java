/*
 * Copyright 2013-2018 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus.jackson;

import org.junit.Test;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.UnknownRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent;
import org.springframework.cloud.bus.event.test.TypedRemoteApplicationEvent;
import org.springframework.messaging.support.MessageBuilder;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		assertTrue("event is wrong type", event instanceof MyRemoteApplicationEvent);
		MyRemoteApplicationEvent myEvent = MyRemoteApplicationEvent.class.cast(event);
		assertEquals("originService was wrong", "myorigin", myEvent.getOriginService());
		assertEquals("destinationService was wrong", "myservice",
				myEvent.getDestinationService());
	}

	@Test
	public void testDeserializeWhenTypeIsKnown() throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		RemoteApplicationEvent event = mapper.readValue("{\"type\":\"another\"}",
				AnotherRemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof AnotherRemoteApplicationEvent);
	}

	@Test
	public void testDeserializeCustomizedObjectMapper() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(mapper);
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"TestRemoteApplicationEvent\", \"origin_service\":\"myorigin\"}").build(),
				RemoteApplicationEvent.class);
		assertThat(event)
				.isNotNull()
				.isInstanceOf(TestRemoteApplicationEvent.class);
		assertThat(TestRemoteApplicationEvent.class.cast(event).getOriginService()).isEqualTo("myorigin");
	}

	@Test
	public void testDeserializeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter();
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"TestRemoteApplicationEvent\"}").build(),
				RemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof TestRemoteApplicationEvent);
	}

	@Test
	public void testDeserializeUnknownTypeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter();
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"NotDefinedTestRemoteApplicationEvent\"}").build(),
				RemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof UnknownRemoteApplicationEvent);
		assertEquals("type information is wrong", "NotDefinedTestRemoteApplicationEvent", ((UnknownRemoteApplicationEvent)event).getTypeInfo());
		assertEquals("payload is wrong", "{\"type\":\"NotDefinedTestRemoteApplicationEvent\"}", ((UnknownRemoteApplicationEvent)event).getPayloadAsString());
	}

	@Test
	public void testDeserializeJsonTypeWithMessageConverter() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter();
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(
				MessageBuilder.withPayload("{\"type\":\"typed\"}").build(),
				RemoteApplicationEvent.class);
		assertTrue("event is wrong type", event instanceof TypedRemoteApplicationEvent);
	}

	/**
	 * see https://github.com/spring-cloud/spring-cloud-bus/issues/74
	 */
	@Test
	public void testDeserializeAckRemoteApplicationEventWithKnownType() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter();
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(MessageBuilder.withPayload(
				"{\"type\":\"AckRemoteApplicationEvent\", \"event\":\"org.springframework.cloud.bus.event.test.TestRemoteApplicationEvent\"}")
				.build(), RemoteApplicationEvent.class);
		assertTrue("event is no ack", event instanceof AckRemoteApplicationEvent);
		AckRemoteApplicationEvent ackEvent = AckRemoteApplicationEvent.class.cast(event);
		assertEquals("inner ack event has wrong type", TestRemoteApplicationEvent.class, ackEvent.getEvent());
	}

	/**
	 * see https://github.com/spring-cloud/spring-cloud-bus/issues/74
	 */
	@Test
	public void testDeserializeAckRemoteApplicationEventWithUnknownType() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter();
		converter.afterPropertiesSet();
		Object event = converter.fromMessage(MessageBuilder.withPayload(
				"{\"type\":\"AckRemoteApplicationEvent\", \"event\":\"foo.bar.TestRemoteApplicationEvent\"}").build(),
				RemoteApplicationEvent.class);
		assertTrue("event is no ack", event instanceof AckRemoteApplicationEvent);
		AckRemoteApplicationEvent ackEvent = AckRemoteApplicationEvent.class.cast(event);
		assertEquals("inner ack event has wrong type", UnknownRemoteApplicationEvent.class, ackEvent.getEvent());
	}

	@SuppressWarnings("serial")
	@JsonTypeName("my")
	public static class MyRemoteApplicationEvent extends RemoteApplicationEvent {
		@SuppressWarnings("unused")
		private MyRemoteApplicationEvent() {
		}

		protected MyRemoteApplicationEvent(Object source, String originService,
				String destinationService) {
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

		protected AnotherRemoteApplicationEvent(Object source, String originService,
				String destinationService) {
			super(source, originService, destinationService);
		}

		protected AnotherRemoteApplicationEvent(Object source, String originService) {
			super(source, originService);
		}
	}
}
