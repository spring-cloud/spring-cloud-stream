package org.springframework.cloud.bus;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.bus.endpoint.BusEndpoint;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.EnvironmentChangeListener;
import org.springframework.cloud.bus.event.RefreshListener;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.context.environment.EnvironmentManager;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.cloud.endpoint.RefreshEndpoint;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.config.ChannelBindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

/**
 * @author Spencer Gibb
 * @author Dave Syer
 */
@Configuration
@ConditionalOnBusEnabled
@EnableBinding(SpringCloudBusClient.class)
@EnableConfigurationProperties(BusProperties.class)
public class BusAutoConfiguration implements ApplicationEventPublisherAware {

	public static final String BUS_PATH_MATCHER_NAME = "busPathMatcher";

	@Autowired
	@Output(SpringCloudBusClient.OUTPUT)
	private MessageChannel cloudBusOutboundChannel;

	@Autowired
	private ServiceMatcher serviceMatcher;

	@Autowired
	private ChannelBindingProperties bindings;

	@Autowired
	private BusProperties bus;

	private ApplicationEventPublisher applicationEventPublisher;

	@PostConstruct
	public void init() {
		Object inputBinding = this.bindings.getBindings().get(SpringCloudBusClient.INPUT);
		if (inputBinding == null || inputBinding instanceof String) {
			this.bindings.getBindings().put(SpringCloudBusClient.INPUT,
					new HashMap<String, Object>());
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> input = (Map<String, Object>) this.bindings.getBindings().get(SpringCloudBusClient.INPUT);
		if (!input.containsKey("destination") || SpringCloudBusClient.INPUT.equals(inputBinding)) {
			input.put("destination", this.bus.getDestination());
		}
		Object outputBinding = this.bindings.getBindings().get(SpringCloudBusClient.OUTPUT);
		if (outputBinding == null || outputBinding instanceof String) {
			this.bindings.getBindings().put(SpringCloudBusClient.OUTPUT,
					new HashMap<String, Object>());
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> output = (Map<String, Object>) this.bindings.getBindings().get(SpringCloudBusClient.OUTPUT);
		if (!output.containsKey("destination") || SpringCloudBusClient.OUTPUT.equals(outputBinding)) {
			output.put("destination", this.bus.getDestination());
		}
	}

	@Override
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@EventListener(classes = RemoteApplicationEvent.class)
	public void acceptLocal(RemoteApplicationEvent event) {
		if (this.serviceMatcher.isFromSelf(event)) {
			this.cloudBusOutboundChannel.send(MessageBuilder.withPayload(event).build());
		}
	}

	@ServiceActivator(inputChannel = SpringCloudBusClient.INPUT)
	public void acceptRemote(RemoteApplicationEvent event) {
		if (!this.serviceMatcher.isFromSelf(event) && this.serviceMatcher.isForSelf(event)
				&& this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
	}

	@Configuration
	protected static class MatcherConfiguration {

		@BusPathMatcher
		// There is a @Bean of type PathMatcher coming from Spring MVC
		@ConditionalOnMissingBean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
		@Bean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
		public PathMatcher busPathMatcher() {
			return new AntPathMatcher(":");
		}

		@Bean
		public ServiceMatcher serviceMatcher(@BusPathMatcher PathMatcher pathMatcher) {
			ServiceMatcher serviceMatcher = new ServiceMatcher();
			serviceMatcher.setMatcher(pathMatcher);
			return serviceMatcher;
		}

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
