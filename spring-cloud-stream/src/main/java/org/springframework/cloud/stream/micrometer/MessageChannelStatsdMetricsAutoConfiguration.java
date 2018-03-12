/*
 * Copyright 2018 the original author or authors.
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
 */

package org.springframework.cloud.stream.micrometer;

import java.util.Collection;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdMeterRegistry;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.statsd.StatsdMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.support.GenericMessage;

/**
 * Emits metrics in the Datadog StatsD line format over a message channel.
 *
 * @author Oleg Zhurakousky
 * @author Jon Schneider
 *
 * @since 2.0
 */
@Configuration
@AutoConfigureBefore({ CompositeMeterRegistryAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class })
@AutoConfigureAfter({ MetricsAutoConfiguration.class, StatsdMetricsExportAutoConfiguration.class })
@ConditionalOnClass({ Binder.class, MetricsAutoConfiguration.class })
@ConditionalOnProperty("spring.cloud.stream.bindings." + MetersPublisherBinding.APPLICATION_METRICS + ".destination")
@EnableConfigurationProperties(ApplicationMetricsProperties.class)
public class MessageChannelStatsdMetricsAutoConfiguration {

	@Bean
	public StatsdMeterRegistry statsdRegistry(ApplicationMetricsProperties applicationMetricsProperties,
			MetersPublisherBinding publisherBinding, Clock clock) {

		StatsdMeterRegistry registry = StatsdMeterRegistry.builder(StatsdConfig.DEFAULT)
				.clock(clock)
				.lineSink(line -> publisherBinding.applicationMetrics().send(new GenericMessage<String>(line)))
				.build();

		registry.config().meterFilter(MeterFilter.denyUnless(id -> id.getName().startsWith("spring.integration")));

		Collection<Tag> tags = applicationMetricsProperties.getExportProperties().entrySet().stream()
				.map(prop -> Tag.of(prop.getKey(), prop.getValue().toString()))
				.collect(Collectors.toList());

		registry.config().commonTags(tags);

		return registry;
	}

	@Bean
	public BeanFactoryPostProcessor metersPublisherBindingRegistrant() {
		return beanFactory -> {
			RootBeanDefinition emitterBindingDefinition = new RootBeanDefinition(BindableProxyFactory.class);
			emitterBindingDefinition.getConstructorArgumentValues().addGenericArgumentValue(MetersPublisherBinding.class);
			((DefaultListableBeanFactory) beanFactory).registerBeanDefinition(MetersPublisherBinding.class.getName(), emitterBindingDefinition);
		};
	}
}
