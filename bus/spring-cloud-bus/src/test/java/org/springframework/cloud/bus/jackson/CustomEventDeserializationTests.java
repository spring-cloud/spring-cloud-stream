/*
 * Copyright 2026-present the original author or authors.
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

import org.junit.Test;

import org.springframework.cloud.bus.custom.KeyEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Custom events that have a public no-arg constructor next to a public constructor
 * taking the source, as application code usually writes them (GH-3162).
 */
public class CustomEventDeserializationTests {

	@Test
	public void deserializeCustomEventWithPublicConstructors() throws Exception {
		BusJacksonMessageConverter converter = new BusJacksonMessageConverter(null);
		converter.setPackagesToScan(new String[] { KeyEvent.class.getPackageName() });
		converter.afterPropertiesSet();

		KeyEvent original = new KeyEvent(this, "origin:1", "destination:**", "some-key");
		byte[] json = converter.getMapper().build().writeValueAsBytes(original);

		Object result = converter.fromMessage(MessageBuilder.withPayload(json).build(), RemoteApplicationEvent.class);

		assertThat(result).isInstanceOf(KeyEvent.class);
		KeyEvent event = (KeyEvent) result;
		assertThat(event.getKey()).isEqualTo("some-key");
		assertThat(event.getOriginService()).isEqualTo("origin:1");
		assertThat(event.getDestinationService()).isEqualTo("destination:**");
		assertThat(event.getId()).isEqualTo(original.getId());
	}

}
