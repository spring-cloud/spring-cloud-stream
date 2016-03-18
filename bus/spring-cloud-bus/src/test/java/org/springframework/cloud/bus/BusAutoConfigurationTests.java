package org.springframework.cloud.bus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.SentApplicationEvent;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
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
		assertNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Test
	public void inboundFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("foo");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		assertNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Test
	public void inboundNotFromSelf() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Test
	public void inboundNotFromSelfWithAck() throws Exception {
		this.context = SpringApplication
				.run(new Object[] { InboundMessageHandlerConfiguration.class,
						OutboundMessageHandlerConfiguration.class,
						SentMessageConfiguration.class }, new String[] {});
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		RefreshRemoteApplicationEvent refresh = this.context
				.getBean(InboundMessageHandlerConfiguration.class).refresh;
		assertNotNull(refresh);
		OutboundMessageHandlerConfiguration outbound = this.context
				.getBean(OutboundMessageHandlerConfiguration.class);
		outbound.latch.await(2000L, TimeUnit.MILLISECONDS);
		String message = (String) outbound.message.getPayload();
		assertTrue("Wrong ackId: " + message,
				message.contains("\"ackId\":\"" + refresh.getId()));
	}

	@Test
	public void inboundNotFromSelfWithTrace() throws Exception {
		this.context = SpringApplication.run(
				new Object[] { InboundMessageHandlerConfiguration.class,
						OutboundMessageHandlerConfiguration.class,
						SentMessageConfiguration.class },
				new String[] { "--spring.cloud.bus.trace.enabled=true" });
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", null)));
		RefreshRemoteApplicationEvent refresh = this.context
				.getBean(InboundMessageHandlerConfiguration.class).refresh;
		assertNotNull(refresh);
		SentMessageConfiguration sent = this.context
				.getBean(SentMessageConfiguration.class);
		assertNotNull(sent.event);
		assertEquals(1, sent.count);
	}

	@Test
	public void inboundAckWithTrace() throws Exception {
		this.context = SpringApplication.run(
				new Object[] { InboundMessageHandlerConfiguration.class,
						OutboundMessageHandlerConfiguration.class,
						AckMessageConfiguration.class },
				new String[] { "--spring.cloud.bus.trace.enabled=true" });
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(new AckRemoteApplicationEvent(this, "foo",
						null, "ID", "bar", RefreshRemoteApplicationEvent.class)));
		AckMessageConfiguration sent = this.context
				.getBean(AckMessageConfiguration.class);
		assertNotNull(sent.event);
		assertEquals(1, sent.count);
	}

	@Test
	public void outboundFromSelf() throws Exception {
		this.context = SpringApplication.run(OutboundMessageHandlerConfiguration.class,
				"--debug=true");
		this.context.setId("foo");
		this.context.publishEvent(new RefreshRemoteApplicationEvent(this, "foo", null));
		OutboundMessageHandlerConfiguration outbound = this.context
				.getBean(OutboundMessageHandlerConfiguration.class);
		outbound.latch.await(2000L, TimeUnit.MILLISECONDS);
		assertNotNull(outbound.message);
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
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Test
	public void inboundNotFromSelfDeepPathPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar:test:1000");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", "bar:**")));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Test
	public void inboundNotFromSelfFlatPattern() {
		this.context = SpringApplication.run(InboundMessageHandlerConfiguration.class);
		this.context.setId("bar");
		this.context.getBean(SpringCloudBusClient.INPUT, MessageChannel.class)
				.send(new GenericMessage<>(
						new RefreshRemoteApplicationEvent(this, "foo", "bar*")));
		assertNotNull(
				this.context.getBean(InboundMessageHandlerConfiguration.class).refresh);
	}

	@Configuration
	@Import({ MessageConsumer.class, BusAutoConfiguration.class,
			TestSupportBinderAutoConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class OutboundMessageHandlerConfiguration {

		@Autowired
		@Output(SpringCloudBusClient.OUTPUT)
		private MessageChannel cloudBusOutboundChannel;

		private CountDownLatch latch = new CountDownLatch(1);

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
					OutboundMessageHandlerConfiguration.this.latch.countDown();
				}
			};
		}

	}

	@Configuration
	@MessageEndpoint
	protected static class MessageConsumer {

		@ServiceActivator(inputChannel = SpringCloudBusClient.OUTPUT)
		public void handle(Message<?> msg) {
		}

	}

	@Configuration
	@Import({ MessageConsumer.class, BusAutoConfiguration.class,
			TestSupportBinderAutoConfiguration.class,
			PropertyPlaceholderAutoConfiguration.class })
	protected static class InboundMessageHandlerConfiguration {

		private RefreshRemoteApplicationEvent refresh;

		@EventListener(RefreshRemoteApplicationEvent.class)
		public void refresh(RefreshRemoteApplicationEvent event) {
			this.refresh = event;
		}

	}

	@Configuration
	protected static class SentMessageConfiguration {
		private SentApplicationEvent event;
		private int count;

		@EventListener
		public void onSend(SentApplicationEvent event) {
			this.event = event;
			this.count++;
		}
	}

	@Configuration
	protected static class AckMessageConfiguration {
		private AckRemoteApplicationEvent event;
		private int count;

		@EventListener
		public void onSend(AckRemoteApplicationEvent event) {
			this.event = event;
			this.count++;
		}
	}

}
