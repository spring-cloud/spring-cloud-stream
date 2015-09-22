package org.springframework.cloud.bus;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.annotation.PostConstruct;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.local.config.LocalBinderAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.GenericMessage;

public class BusAutoConfigurationTests {

	private ConfigurableApplicationContext context;

	@After
	public void close() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void inboundNotForSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("foo");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "bar", "bar")));
		assertNull(this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("foo");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		assertNull(this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void outboundFromSelf() throws Exception {
		this.context = SpringApplication.run(OutboundMessageHandlerConfiguration.class,
				"--debug=true");
		this.context.setId("foo");
		this.context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", null));
		Thread.sleep(2000L);
		assertNotNull(
				this.context.getBean(OutboundMessageHandlerConfiguration.class).message);
	}

	@Test
	public void outboundNotFromSelf() {
		this.context = SpringApplication.run(OutboundMessageHandlerConfiguration.class);
		this.context.setId("bar");
		this.context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", null));
		assertNull(
				this.context.getBean(OutboundMessageHandlerConfiguration.class).message);
	}

	@Test
	public void inboundNotFromSelfPathPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar:1000");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", "bar:*")));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelfDeepPathPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar:test:1000");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", "bar:**")));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Test
	public void inboundNotFromSelfFlatPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", "bar*")));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).event);
	}

	@Configuration
	@Import({ BusAutoConfiguration.class, LocalBinderAutoConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class OutboundMessageHandlerConfiguration {

		@Autowired
		@Output(SpringCloudBusClient.OUTPUT)
		private MessageChannel cloudBusOutboundChannel;

		private Message<?> message;

		@PostConstruct
		public void init() {
			((DirectChannel) this.cloudBusOutboundChannel).addInterceptor(interceptor());
		}

		private ChannelInterceptor interceptor() {
			return new ChannelInterceptorAdapter() {
				@Override
				public void postSend(Message<?> message, MessageChannel channel,
						boolean sent) {
					OutboundMessageHandlerConfiguration.this.message = message;
				}
			};
		}

	}

	@Configuration
	@Import({ BusAutoConfiguration.class, LocalBinderAutoConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class InboundMessageHandlerConfiguration
			implements ApplicationListener<RefreshRemoteApplicationEvent> {

		private RefreshRemoteApplicationEvent event;

		@Override
		public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
			this.event = event;
		}

	}

}
