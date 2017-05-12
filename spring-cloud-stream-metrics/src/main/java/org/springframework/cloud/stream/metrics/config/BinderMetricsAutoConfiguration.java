/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics.config;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.actuate.autoconfigure.MetricExportAutoConfiguration;
import org.springframework.boot.actuate.endpoint.MetricsEndpoint;
import org.springframework.boot.actuate.metrics.export.Exporter;
import org.springframework.boot.actuate.metrics.export.MetricExporters;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.metrics.ApplicationMetricsExporter;
import org.springframework.cloud.stream.metrics.ApplicationMetricsProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Autoconfiguration registering an {@link Exporter} that publishes application metrics
 * over the {@link Emitter#applicationMetrics()} channel.
 *
 * @author Vinicius Carvalho
 * @author Marius Bogoevici
 *
 */
@Configuration
@ConditionalOnClass(Binder.class)
@EnableBinding(Emitter.class)
@EnableConfigurationProperties(ApplicationMetricsProperties.class)
@AutoConfigureAfter(MetricExportAutoConfiguration.class)
@ConditionalOnProperty("spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS
		+ ".destination")
public class BinderMetricsAutoConfiguration {

	public static final String APPLICATION_METRICS_EXPORTER_TRIGGER_NAME = "application";

	public static Log log = LogFactory.getLog(BinderMetricsAutoConfiguration.class);

	/**
	 * Postprocessor for installing the {@link ApplicationMetricsExporter} as an exporter
	 * under the name {@code application}.
	 * @param endpoint the metrics endpoint (lazy reference to prevent early
	 * initialization)
	 * @param emitter the emitter bound interface
	 * @param properties application metrics properties
	 * @return
	 */
	@Bean
	public static BeanPostProcessor metricExportersBeanPostProcessor(final @Lazy MetricsEndpoint endpoint,
			final Emitter emitter, final ApplicationMetricsProperties properties) {
		return new BeanPostProcessor() {
			@Override
			public Object postProcessBeforeInitialization(Object bean, String name) throws BeansException {
				return bean;
			}

			@Override
			public Object postProcessAfterInitialization(Object bean, String name) throws BeansException {
				if (bean instanceof MetricExporters) {
					Map<String, Exporter> exporters = ((MetricExporters) bean).getExporters();
					if (!exporters.containsKey(APPLICATION_METRICS_EXPORTER_TRIGGER_NAME)) {
						exporters.put(APPLICATION_METRICS_EXPORTER_TRIGGER_NAME,
								new ApplicationMetricsExporter(endpoint, emitter.applicationMetrics(), properties));
					}
					else {
						log.warn("Could not register ApplicationMetricExporter: "
								+ exporters.get(APPLICATION_METRICS_EXPORTER_TRIGGER_NAME)
								+ " was already registered as " + APPLICATION_METRICS_EXPORTER_TRIGGER_NAME);
					}
				}
				return bean;
			}
		};
	}

	@Bean
	public MetricJsonSerializer metricJsonSerializer() {
		return new MetricJsonSerializer();
	}

}
