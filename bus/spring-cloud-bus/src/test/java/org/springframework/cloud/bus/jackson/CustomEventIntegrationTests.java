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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.custom.KeyEvent;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A custom event sent by another service reaches the listeners of the application
 * (GH-3162).
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = CustomEventIntegrationTests.MyApp.class,
		properties = { "spring.application.name=listener", "spring.cloud.bus.id=listener:0" })
public class CustomEventIntegrationTests {

	@Autowired
	private InputDestination input;

	@Autowired
	@Qualifier("busJsonConverter")
	private BusJacksonMessageConverter converter;

	@Autowired
	private KeyEventListener listener;

	@Test
	public void customEventSentByAnotherServiceIsDelivered() throws Exception {
		KeyEvent sent = new KeyEvent(this, "other:1", "**", "some-key");
		byte[] json = this.converter.getMapper().build().writeValueAsBytes(sent);

		this.input.send(MessageBuilder.withPayload(json).setHeader("contentType", "application/json").build(),
				"springCloudBus");

		assertThat(this.listener.received).hasSize(1);
		KeyEvent received = this.listener.received.get(0);
		assertThat(received.getKey()).isEqualTo("some-key");
		assertThat(received.getOriginService()).isEqualTo("other:1");
		assertThat(received.getId()).isEqualTo(sent.getId());
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	@Import(TestChannelBinderConfiguration.class)
	@RemoteApplicationEventScan(basePackageClasses = KeyEvent.class)
	static class MyApp {

		@Bean
		KeyEventListener keyEventListener() {
			return new KeyEventListener();
		}

	}

	static class KeyEventListener implements ApplicationListener<KeyEvent> {

		final List<KeyEvent> received = new CopyOnWriteArrayList<>();

		@Override
		public void onApplicationEvent(KeyEvent event) {
			this.received.add(event);
		}

	}

}
