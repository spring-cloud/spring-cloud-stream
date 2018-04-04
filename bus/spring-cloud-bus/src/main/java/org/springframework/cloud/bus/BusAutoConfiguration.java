/*
 * Copyright 2013-2017 the original author or authors.
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

package org.springframework.cloud.bus;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnEnabledEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.trace.http.HttpTraceRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.EnvironmentChangeListener;
import org.springframework.cloud.bus.event.RefreshListener;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.SentApplicationEvent;
import org.springframework.cloud.bus.event.TraceListener;
import org.springframework.cloud.context.environment.EnvironmentManager;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
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
	private BindingServiceProperties bindings;

	@Autowired
	private BusProperties bus;

	private ApplicationEventPublisher applicationEventPublisher;

	@PostConstruct
	public void init() {
		BindingProperties inputBinding = this.bindings.getBindings()
				.get(SpringCloudBusClient.INPUT);
		if (inputBinding == null) {
			this.bindings.getBindings().put(SpringCloudBusClient.INPUT,
					new BindingProperties());
		}
		BindingProperties input = this.bindings.getBindings()
				.get(SpringCloudBusClient.INPUT);
		if (input.getDestination() == null) {
			input.setDestination(this.bus.getDestination());
		}
		BindingProperties outputBinding = this.bindings.getBindings()
				.get(SpringCloudBusClient.OUTPUT);
		if (outputBinding == null) {
			this.bindings.getBindings().put(SpringCloudBusClient.OUTPUT,
					new BindingProperties());
		}
		BindingProperties output = this.bindings.getBindings()
				.get(SpringCloudBusClient.OUTPUT);
		if (output.getDestination() == null) {
			output.setDestination(this.bus.getDestination());
		}
	}

	@Override
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@EventListener(classes = RemoteApplicationEvent.class)
	public void acceptLocal(RemoteApplicationEvent event) {
		if (this.serviceMatcher.isFromSelf(event)
				&& !(event instanceof AckRemoteApplicationEvent)) {
			this.cloudBusOutboundChannel.send(MessageBuilder.withPayload(event).build());
		}
	}

	@StreamListener(SpringCloudBusClient.INPUT)
	public void acceptRemote(RemoteApplicationEvent event) {
		if (event instanceof AckRemoteApplicationEvent) {
			if (this.bus.getTrace().isEnabled() && !this.serviceMatcher.isFromSelf(event)
					&& this.applicationEventPublisher != null) {
				this.applicationEventPublisher.publishEvent(event);
			}
			// If it's an ACK we are finished processing at this point
			return;
		}
		if (this.serviceMatcher.isForSelf(event)
				&& this.applicationEventPublisher != null) {
			if (!this.serviceMatcher.isFromSelf(event)) {
				this.applicationEventPublisher.publishEvent(event);
			}
			if (this.bus.getAck().isEnabled()) {
				AckRemoteApplicationEvent ack = new AckRemoteApplicationEvent(this,
						this.serviceMatcher.getServiceId(),
						this.bus.getAck().getDestinationService(),
						event.getDestinationService(), event.getId(), event.getClass());
				this.cloudBusOutboundChannel
						.send(MessageBuilder.withPayload(ack).build());
				this.applicationEventPublisher.publishEvent(ack);
			}
		}
		if (this.bus.getTrace().isEnabled() && this.applicationEventPublisher != null) {
			// We are set to register sent events so publish it for local consumption,
			// irrespective of the origin
			this.applicationEventPublisher.publishEvent(new SentApplicationEvent(this,
					event.getOriginService(), event.getDestinationService(),
					event.getId(), event.getClass()));
		}
	}

	@Configuration
	protected static class MatcherConfiguration {

		@BusPathMatcher
		// There is a @Bean of type PathMatcher coming from Spring MVC
		@ConditionalOnMissingBean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
		@Bean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
		public PathMatcher busPathMatcher() {
			return new DefaultBusPathMatcher(new AntPathMatcher(":"));
		}

		@Bean
		public ServiceMatcher serviceMatcher(@BusPathMatcher PathMatcher pathMatcher,
				BusProperties properties) {
			ServiceMatcher serviceMatcher = new ServiceMatcher(pathMatcher, properties.getId());
			return serviceMatcher;
		}

	}

	@Bean
	@ConditionalOnProperty(value = "spring.cloud.bus.refresh.enabled", matchIfMissing = true)
	@ConditionalOnBean(ContextRefresher.class)
	public RefreshListener refreshListener(ContextRefresher contextRefresher) {
		return new RefreshListener(contextRefresher);
	}

	@Configuration
	@ConditionalOnClass({ Endpoint.class, RefreshScope.class })
	protected static class BusRefreshConfiguration {

		@Configuration
		@ConditionalOnBean(ContextRefresher.class)
		@ConditionalOnProperty(value = "endpoints.spring.cloud.bus.refresh.enabled", matchIfMissing = true)
		protected static class BusRefreshEndpointConfiguration {
			@Bean
			public RefreshBusEndpoint refreshBusEndpoint(ApplicationContext context,
					BusProperties bus) {
				return new RefreshBusEndpoint(context, bus.getId());
			}
		}

	}

	@Configuration
	@ConditionalOnClass({ Endpoint.class })
	@ConditionalOnBean(HttpTraceRepository.class)
	@ConditionalOnProperty(value = "spring.cloud.bus.trace.enabled", matchIfMissing = false)
	protected static class BusAckTraceConfiguration {

		@Bean
		@ConditionalOnMissingBean
		public TraceListener ackTraceListener(HttpTraceRepository repository) {
			return new TraceListener(repository);
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
		protected static class EnvironmentBusEndpointConfiguration {
			@Bean
			@ConditionalOnEnabledEndpoint
			public EnvironmentBusEndpoint environmentBusEndpoint(
					ApplicationContext context, BusProperties bus) {
				return new EnvironmentBusEndpoint(context, bus.getId());
			}
		}
	}

}
