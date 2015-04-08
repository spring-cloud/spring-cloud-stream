package org.springframework.cloud.bus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bootstrap.config.RefreshEndpoint;
import org.springframework.cloud.bus.endpoint.BusEndpoint;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.EnvironmentChangeListener;
import org.springframework.cloud.bus.event.RefreshListener;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.context.environment.EnvironmentManager;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.integration.core.GenericSelector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.event.inbound.ApplicationEventListeningMessageProducer;
import org.springframework.integration.event.outbound.ApplicationEventPublishingMessageHandler;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

/**
 * @author Spencer Gibb
 * @author Dave Syer
 */
@Configuration
@ConditionalOnBusEnabled
public class BusAutoConfiguration {

	@Autowired
	private ConfigurableApplicationContext context;

	@Bean
	public SubscribableChannel cloudBusOutboundChannel() {
		return new DirectChannel();
	}

	// TODO: is there a way to move these filters to rabbit while not losing the
	// information once it is published to spring?
	@Bean
	public GenericSelector<?> outboundFilter() {
		return new GenericSelector<RemoteApplicationEvent>() {
			@Override
			public boolean accept(RemoteApplicationEvent source) {
				return serviceMatcher().isFromSelf(source);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private ApplicationEventListeningMessageProducer cloudBusOutboundMessageProducer() {
		ApplicationEventListeningMessageProducer producer = new ApplicationEventListeningMessageProducer();
		producer.setEventTypes(RemoteApplicationEvent.class);
		return producer;
	}

	@Bean
	public IntegrationFlow cloudBusOutboundFlow() {
		ApplicationEventListeningMessageProducer producer = cloudBusOutboundMessageProducer();
		// Workaround for bug in IntegrationFlow (it won't register the listener)
		context.addApplicationListener(producer);
		return IntegrationFlows.from(producer).filter(outboundFilter())
				.channel(cloudBusOutboundChannel()).get();
	}

	@Bean
	public MessageChannel cloudBusInboundChannel() {
		return new DirectChannel();
	}

	@Bean
	public GenericSelector<?> inboundFilter() {
		return new GenericSelector<RemoteApplicationEvent>() {
			@Override
			public boolean accept(RemoteApplicationEvent event) {
				return !serviceMatcher().isFromSelf(event) && serviceMatcher().isForSelf(event);
			}
		};
	}

	@Bean
	public IntegrationFlow cloudBusInboundFlow() {
		ApplicationEventPublishingMessageHandler messageHandler = new ApplicationEventPublishingMessageHandler();
		return IntegrationFlows.from(cloudBusInboundChannel()).filter(inboundFilter())
				.handle(messageHandler).get();
	}

	@Bean
	@GlobalChannelInterceptor(patterns = "cloudBusInboundFlow*")
	public WireTap wireTap() {
		return new WireTap(cloudBusWiretapChannel());
	}

	@Bean
	public DirectChannel cloudBusWiretapChannel() {
		return MessageChannels.direct().get();
	}

	@Bean
	public IntegrationFlow loggingFlow() {
		LoggingHandler handler = new LoggingHandler("INFO");
		handler.setShouldLogFullMessage(true);
		return IntegrationFlows.from(cloudBusWiretapChannel()).handle(handler).get();
	}

    @Bean
    public PathMatcher busPathMatcher() {
        return new AntPathMatcher(":");
    }

    @Bean
    public ServiceMatcher serviceMatcher() {
        return new ServiceMatcher();
    }

	@Configuration
	@ConditionalOnClass(Endpoint.class)
	protected static class BusEndpointConfiguration {
		@Bean
		public BusEndpoint busEndpoint() {
			return new BusEndpoint();
		}
	}

	@Configuration
	@ConditionalOnClass({ Endpoint.class, RefreshScope.class })
	@ConditionalOnBean(RefreshEndpoint.class)
	protected static class BusRefreshConfiguration {

		@Bean
		@ConditionalOnProperty(value = "spring.cloud.bus.refresh.enabled", matchIfMissing = true)
		public RefreshListener refreshListener(RefreshEndpoint refreshEndpoint) {
			return new RefreshListener(refreshEndpoint);
		}

		@Configuration
		@ConditionalOnProperty(value = "endpoints.spring.cloud.bus.refresh.enabled", matchIfMissing = true)
		protected static class BusRefreshEndpointConfiguration {
			@Bean
			public RefreshBusEndpoint refreshBusEndpoint(ApplicationContext context,
					BusEndpoint busEndpoint) {
				return new RefreshBusEndpoint(context, context.getId(), busEndpoint);
			}
		}

	}

	@Configuration
	@ConditionalOnClass(EnvironmentManager.class)
	@ConditionalOnBean(EnvironmentManager.class)
	protected static class BusEnvironmentConfiguration {
		@Bean
		@ConditionalOnProperty(value = "spring.cloud.bus.env.enabled", matchIfMissing = true)
		public EnvironmentChangeListener environmentChangeListener() {
			return new EnvironmentChangeListener();
		}

		@Configuration
		@ConditionalOnClass(Endpoint.class)
		@ConditionalOnProperty(value = "endpoints.spring.cloud.bus.env.enabled", matchIfMissing = true)
		protected static class EnvironmentBusEndpointConfiguration {
			@Bean
			public EnvironmentBusEndpoint environmentBusEndpoint(
					ApplicationContext context, BusEndpoint busEndpoint) {
				return new EnvironmentBusEndpoint(context, context.getId(), busEndpoint);
			}
		}
	}



}
