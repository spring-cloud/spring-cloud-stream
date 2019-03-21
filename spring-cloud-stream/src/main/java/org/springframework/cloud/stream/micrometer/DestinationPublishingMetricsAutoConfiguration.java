/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.MeterFilter;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.PatternMatchUtils;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @since 2.0
 */
@Configuration
@AutoConfigureBefore(SimpleMetricsExportAutoConfiguration.class)
@AutoConfigureAfter(MetricsAutoConfiguration.class)
@ConditionalOnClass({ Binder.class, MetricsAutoConfiguration.class })
@ConditionalOnProperty("spring.cloud.stream.bindings."
		+ MetersPublisherBinding.APPLICATION_METRICS + ".destination")
@EnableConfigurationProperties(ApplicationMetricsProperties.class)
public class DestinationPublishingMetricsAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public MetricsPublisherConfig metricsPublisherConfig(
			ApplicationMetricsProperties metersPublisherProperties) {
		return new MetricsPublisherConfig(metersPublisherProperties);
	}

	@Bean
	@ConditionalOnMissingBean
	public DefaultDestinationPublishingMeterRegistry defaultDestinationPublishingMeterRegistry(
			ApplicationMetricsProperties applicationMetricsProperties,
			MetersPublisherBinding publisherBinding,
			MetricsPublisherConfig metricsPublisherConfig, Clock clock) {
		DefaultDestinationPublishingMeterRegistry registry = new DefaultDestinationPublishingMeterRegistry(
				applicationMetricsProperties, publisherBinding, metricsPublisherConfig,
				clock);

		if (StringUtils.hasText(applicationMetricsProperties.getMeterFilter())) {
			registry.config()
					.meterFilter(MeterFilter.denyUnless(id -> PatternMatchUtils
							.simpleMatch(applicationMetricsProperties.getMeterFilter(),
									id.getName())));
		}
		return registry;
	}

	@Bean
	public BeanFactoryPostProcessor metersPublisherBindingRegistrant() {
		return new BeanFactoryPostProcessor() {
			@Override
			public void postProcessBeanFactory(
					ConfigurableListableBeanFactory beanFactory) throws BeansException {
				RootBeanDefinition emitterBindingDefinition = new RootBeanDefinition(
						BindableProxyFactory.class);
				emitterBindingDefinition.getConstructorArgumentValues()
						.addGenericArgumentValue(MetersPublisherBinding.class);
				((DefaultListableBeanFactory) beanFactory).registerBeanDefinition(
						MetersPublisherBinding.class.getName(), emitterBindingDefinition);
			}
		};
	}

}
