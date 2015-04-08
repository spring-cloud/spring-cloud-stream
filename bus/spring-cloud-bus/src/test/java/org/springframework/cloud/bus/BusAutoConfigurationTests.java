package org.springframework.cloud.bus;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

public class BusAutoConfigurationTests {

	private ConfigurableApplicationContext context;

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void inboundNotForSelf() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("foo");
		context.getBean("cloudBusInboundChannel", MessageChannel.class).send(
				new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "bar", "bar")));
		assertNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundFromSelf() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("foo");
		context.getBean("cloudBusInboundChannel", MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo",
						null)));
		assertNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelf() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("bar");
		context.getBean("cloudBusInboundChannel", MessageChannel.class)
				.send(new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo",
						null)));
		assertNotNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void outboundFromSelf() {
		context = SpringApplication.run(OutboundMessageHandlerConfiguration.class);
		context.setId("foo");
		context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", null));
		assertNotNull(context.getBean(OutboundMessageHandlerConfiguration.class).message);
	}

	@Test
	public void outboundNotFromSelf() {
		context = SpringApplication.run(OutboundMessageHandlerConfiguration.class);
		context.setId("bar");
		context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", null));
		assertNull(context.getBean(OutboundMessageHandlerConfiguration.class).message);
	}

	@Test
	public void inboundNotFromSelfPathPattern() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("bar:1000");
		context.getBean("cloudBusInboundChannel", MessageChannel.class).send(
				new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo",
						"bar:*")));
		assertNotNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelfDeepPathPattern() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("bar:test:1000");
		context.getBean("cloudBusInboundChannel", MessageChannel.class).send(
				new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo",
						"bar:**")));
		assertNotNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelfFlatPattern() {
		context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		context.setId("bar");
		context.getBean("cloudBusInboundChannel", MessageChannel.class).send(
				new GenericMessage<>(new RefreshRemoteApplicationEvent(this, "foo",
						"bar*")));
		assertNotNull(context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Configuration
	@Import(BusAutoConfiguration.class)
	@MessageEndpoint
	@EnableIntegration
	protected static class OutboundMessageHandlerConfiguration {

		private Message<?> message;

		@ServiceActivator(inputChannel = "cloudBusOutboundChannel")
		public void handle(Message<?> message) {
			this.message = message;
		}

	}

	@Configuration
	@Import(BusAutoConfiguration.class)
	@MessageEndpoint
	@EnableIntegration
	protected static class InboundMessageHandlerConfiguration implements
			ApplicationListener<RefreshRemoteApplicationEvent> {

		private RefreshRemoteApplicationEvent event;

		@Override
		public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
			this.event = event;
		}

	}

}
