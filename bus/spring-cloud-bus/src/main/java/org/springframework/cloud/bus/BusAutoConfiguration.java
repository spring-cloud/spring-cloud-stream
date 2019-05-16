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

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnEnabledEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.trace.http.HttpTraceRepository;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.autoconfigure.LifecycleMvcEndpointAutoConfiguration;
import org.springframework.cloud.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.cloud.bus.event.AckRemoteApplicationEvent;
import org.springframework.cloud.bus.event.EnvironmentChangeListener;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.event.SentApplicationEvent;
import org.springframework.cloud.bus.event.TraceListener;
import org.springframework.cloud.context.environment.EnvironmentManager;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
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
@AutoConfigureBefore(BindingServiceConfiguration.class)
// so stream bindings work properly
@AutoConfigureAfter(LifecycleMvcEndpointAutoConfiguration.class)
// so actuator endpoints have needed dependencies
public class BusAutoConfiguration implements ApplicationEventPublisherAware {

	/**
	 * Name of the Bus path matcher.
	 */
	public static final String BUS_PATH_MATCHER_NAME = "busPathMatcher";

	/**
	 * Name of the Spring Cloud Config property.
	 */
	public static final String CLOUD_CONFIG_NAME_PROPERTY = "spring.cloud.config.name";

	private final ServiceMatcher serviceMatcher;

	private final BindingServiceProperties bindings;

	private final BusProperties bus;

	private MessageChannel cloudBusOutboundChannel;

	private ApplicationEventPublisher applicationEventPublisher;

	public BusAutoConfiguration(ServiceMatcher serviceMatcher,
			BindingServiceProperties bindings, BusProperties bus) {
		this.serviceMatcher = serviceMatcher;
		this.bindings = bindings;
		this.bus = bus;
	}

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
		if (input.getDestination() == null
				|| input.getDestination().equals(SpringCloudBusClient.INPUT)) {
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
		if (output.getDestination() == null
				|| output.getDestination().equals(SpringCloudBusClient.OUTPUT)) {
			output.setDestination(this.bus.getDestination());
		}
	}

	@Override
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Autowired
	@Output(SpringCloudBusClient.OUTPUT)
	public void setCloudBusOutboundChannel(MessageChannel cloudBusOutboundChannel) {
		this.cloudBusOutboundChannel = cloudBusOutboundChannel;
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
				BusProperties properties, Environment environment) {
			String[] configNames = environment.getProperty(CLOUD_CONFIG_NAME_PROPERTY,
					String[].class, new String[] {});
			ServiceMatcher serviceMatcher = new ServiceMatcher(pathMatcher,
					properties.getId(), configNames);
			return serviceMatcher;
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
