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

package org.springframework.cloud.bus;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.PathDestinationFactory;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.SentApplicationEvent;
import org.springframework.cloud.bus.event.UnknownRemoteApplicationEvent;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BusAutoConfigurationTests {

	private ConfigurableApplicationContext context;

	@After
	public void close() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void defaultId() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--server.port=0");
		assertThat(this.context.getBean(BusProperties.class).getId().startsWith("application:0:"))
				.as("Wrong ID: " + this.context.getBean(BusProperties.class).getId()).isTrue();
	}

	@Test
	public void inboundNotForSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=foo",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "bar", "bar")));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNull();
	}

	@Test
	public void inboundFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=foo",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", (String) null)));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNull();
	}

	@Test
	public void inboundNotFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=bar",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", (String) null)));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNotNull();
	}

	@Test
	public void inboundNotFromSelfWithAck() throws Exception {
		this.context = SpringApplication.run(
				new Class[] { InboundMessageHandlerConfiguration.class, OutboundMessageHandlerConfiguration.class,
						SentMessageConfiguration.class },
				new String[] { "--spring.cloud.bus.id=bar", "--server.port=0",
						"--spring.main.allow-bean-definition-overriding=true" });
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", (String) null)));
		RefreshRemoteApplicationEvent refresh = this.context.getBean(InboundMessageHandlerConfiguration.class).refresh;
		assertThat(refresh).isNotNull();
		TestStreamBusBridge busBridge = this.context.getBean(TestStreamBusBridge.class);
		busBridge.latch.await(200, TimeUnit.SECONDS);
		assertThat(busBridge.message).isInstanceOf(AckRemoteApplicationEvent.class);
		AckRemoteApplicationEvent message = (AckRemoteApplicationEvent) busBridge.message;
		assertThat(message.getAckId()).as("Wrong ackId: %s", message).isEqualTo(refresh.getId());
	}

	@Test
	public void inboundNotFromSelfWithTrace() {
		this.context = SpringApplication.run(
				new Class[] { InboundMessageHandlerConfiguration.class, OutboundMessageHandlerConfiguration.class,
						SentMessageConfiguration.class },
				new String[] { "--spring.cloud.bus.trace.enabled=true", "--spring.cloud.bus.id=bar",
						"--server.port=0" });
		this.context.getBean(BusConsumer.class).accept(new RefreshRemoteApplicationEvent(this, "foo", (String) null));
		RefreshRemoteApplicationEvent refresh = this.context.getBean(InboundMessageHandlerConfiguration.class).refresh;
		assertThat(refresh).isNotNull();
		SentMessageConfiguration sent = this.context.getBean(SentMessageConfiguration.class);
		assertThat(sent.event).isNotNull();
		assertThat(sent.count).isEqualTo(1);
	}

	@Test
	public void inboundAckWithTrace() throws InterruptedException {
		this.context = SpringApplication.run(
				new Class[] { InboundMessageHandlerConfiguration.class, OutboundMessageHandlerConfiguration.class,
						AckMessageConfiguration.class },
				new String[] { "--spring.cloud.bus.trace.enabled=true", "--spring.cloud.bus.id=bar",
						"--server.port=0" });
		this.context.getBean(BusConsumer.class).accept(new AckRemoteApplicationEvent(this, "foo",
				new PathDestinationFactory().getDestination(null), "ID", "bar", RefreshRemoteApplicationEvent.class));
		AckMessageConfiguration ack = this.context.getBean(AckMessageConfiguration.class);
		assertThat(ack.latch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(ack.event).isNotNull();
		assertThat(ack.count).isEqualTo(1);
	}

	@Test
	public void outboundFromSelf() throws Exception {
		this.context = SpringApplication.run(OutboundMessageHandlerConfiguration.class, "--debug=true",
				"--spring.cloud.bus.id=foo", "--server.port=0");
		this.context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", (String) null));
		TestStreamBusBridge busBridge = this.context.getBean(TestStreamBusBridge.class);
		busBridge.latch.await(2, TimeUnit.SECONDS);
		assertThat(busBridge.message).as("message was null").isNotNull();
	}

	@Test
	public void outboundNotFromSelf() {
		this.context = SpringApplication.run(OutboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=bar",
				"--server.port=0");
		this.context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", (String) null));
		assertThat(this.context.getBean(TestStreamBusBridge.class).message).isNull();
	}

	@Test
	public void inboundNotFromSelfPathPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=bar:1000",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", "bar:*")));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNotNull();
	}

	@Test
	public void inboundNotFromSelfDeepPathPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class,
				"--spring.cloud.bus.id=bar:test:1000", "--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", "bar:**")));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNotNull();
	}

	@Test
	public void inboundNotFromSelfFlatPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=bar",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo", "bar*")));
		assertThat(this.context.getBean(InboundMessageHandlerConfiguration.class).refresh).isNotNull();
	}

	// see https://github.com/spring-cloud/spring-cloud-bus/issues/74
	@Test
	public void inboundNotFromSelfUnknown() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class, "--spring.cloud.bus.id=bar",
				"--server.port=0");
		this.context.getBean(BusConstants.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new UnknownRemoteApplicationEvent(this, "UnknownEvent", "yada".getBytes())));
		// No Exception expected
	}

	@Test
	public void initDoesNotOverrideCustomDestination() {
		HashMap<String, BindingProperties> properties = new HashMap<>();
		BindingProperties input = new BindingProperties();
		input.setDestination("mydestination");
		properties.put(BusConstants.INPUT, input);
		BindingProperties output = new BindingProperties();
		output.setDestination("mydestination");
		properties.put(BusConstants.OUTPUT, output);

		setupBusAutoConfig(properties);

		BindingProperties inputProps = properties.get(BusConstants.INPUT);
		assertThat(inputProps.getDestination()).isEqualTo("mydestination");

		BindingProperties outputProps = properties.get(BusConstants.OUTPUT);
		assertThat(outputProps.getDestination()).isEqualTo("mydestination");
	}

	private BusProperties setupBusAutoConfig(HashMap<String, BindingProperties> properties) {
		BindingServiceProperties serviceProperties = mock(BindingServiceProperties.class);
		when(serviceProperties.getBindings()).thenReturn(properties);

		BusProperties bus = new BusProperties();
		BusAutoConfiguration configuration = new BusAutoConfiguration();
		return bus;
	}

	// see https://github.com/spring-cloud/spring-cloud-bus/issues/101
	@Test
	public void serviceMatcherIdIsConstantAfterRefresh() {
		this.context = SpringApplication.run(new Class[] { RefreshConfig.class, TestChannelBinderConfiguration.class },
				new String[] { "--server.port=0", "--spring.main.allow-bean-definition-overriding=true" });
		String originalServiceId = this.context.getBean(ServiceMatcher.class).getBusId();
		this.context.getBean(ContextRefresher.class).refresh();
		String newServiceId = this.context.getBean(ServiceMatcher.class).getBusId();
		assertThat(newServiceId).isEqualTo(originalServiceId);
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	protected static class RefreshConfig {

	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	@ImportAutoConfiguration({ BusAutoConfiguration.class, TestChannelBinderConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class OutboundMessageHandlerConfiguration {

		@Bean
		@Primary
		StreamBusBridge testStreamBusBridge(StreamBridge streamBridge, BusProperties properties) {
			return new TestStreamBusBridge(streamBridge, properties);
		}

	}

	protected static class TestStreamBusBridge extends StreamBusBridge {

		private CountDownLatch latch = new CountDownLatch(1);

		private RemoteApplicationEvent message;

		public TestStreamBusBridge(StreamBridge streamBridge, BusProperties properties) {
			super(streamBridge, properties);
		}

		@Override
		public void send(RemoteApplicationEvent event) {
			latch.countDown();
			message = event;
			super.send(event);
		}

	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	@ImportAutoConfiguration({ BusAutoConfiguration.class, TestChannelBinderConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class InboundMessageHandlerConfiguration
			implements ApplicationListener<RefreshRemoteApplicationEvent> {

		private RefreshRemoteApplicationEvent refresh;

		@Override
		public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
			this.refresh = event;
		}

	}

	@Configuration(proxyBeanMethods = false)
	protected static class SentMessageConfiguration implements ApplicationListener<SentApplicationEvent> {

		private SentApplicationEvent event;

		private int count;

		@Override
		public void onApplicationEvent(SentApplicationEvent event) {
			this.event = event;
			this.count++;
		}

	}

	@Configuration(proxyBeanMethods = false)
	protected static class AckMessageConfiguration implements ApplicationListener<AckRemoteApplicationEvent> {

		private CountDownLatch latch = new CountDownLatch(1);

		private AckRemoteApplicationEvent event;

		private int count;

		@Override
		public void onApplicationEvent(AckRemoteApplicationEvent event) {
			this.event = event;
			this.count++;
			latch.countDown();
		}

	}

}
