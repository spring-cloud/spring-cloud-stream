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

package org.springframework.cloud.stream.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.MetricsEndpoint;
import org.springframework.boot.actuate.endpoint.MetricsEndpointMetricReader;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.actuate.metrics.export.MetricCopyExporter;
import org.springframework.boot.bind.RelaxedNames;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.config.metrics.StreamMetricsProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * @author Vinicius Carvalho
 *
 * Component that sends metrics from {@link MetricsEndpointMetricReader} downstream via the configured metrics channel.
 *
 * It uses {@link Scheduled} support to periodially emit messages polled from the endpoint.
 */
public class BinderMetricsEmitter implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {

	@Autowired
	private Emitter source;

	@Autowired
	private StreamMetricsProperties properties;

	@Autowired
	private BindingServiceProperties bindingServiceProperties;

	private MetricsEndpointMetricReader metricsReader;

	private ApplicationContext applicationContext;

	/**
	 * List of properties that are going to be appended to each message.
	 * This gets populate by onApplicationEvent, once the context refreshes to avoid overhead of doing per message basis.
	 */
	private Map<String,Object> whitelistedProperties;

	public BinderMetricsEmitter(MetricsEndpoint endpoint){
		this.metricsReader = new MetricsEndpointMetricReader(endpoint);
		this.whitelistedProperties = new HashMap<>();
	}

	@Scheduled(fixedRateString = "${spring.cloud.stream.metrics.delay-millis:5000}")
	public void sendMetrics(){
		ApplicationMetrics appMetrics = new ApplicationMetrics(this.properties.getMetricName(),
				this.bindingServiceProperties.getInstanceIndex(),
				filter());
		appMetrics.setProperties(whitelistedProperties);
		source.metrics().send(MessageBuilder.withPayload(appMetrics).build());
	}

	/**
	 * Shameless copied from {@link MetricCopyExporter}
	 * @return
	 */
	protected Collection<Metric> filter(){
		Collection<Metric> result = new ArrayList<>();
		Iterable<Metric<?>> metrics = metricsReader.findAll();
		for(Metric metric : metrics){
			if(isMatch(metric.getName(),this.properties.getIncludes(),this.properties.getExcludes())){
				result.add(metric);
			}
		}
		return result;
	}

	/**
	 * Shameless copied from {@link MetricCopyExporter}
	 * @return
	 */
	private boolean isMatch(String name, String[] includes, String[] excludes) {
		if (ObjectUtils.isEmpty(includes)
				|| PatternMatchUtils.simpleMatch(includes, name)) {
			return !PatternMatchUtils.simpleMatch(excludes, name);
		}
		return false;
	}

	@Override
	/**
	 * Iterates over all property sources from this application context and copies the ones listed in {@link StreamMetricsProperties} includes
	 */
	public void onApplicationEvent(ContextRefreshedEvent event) {
		ConfigurableApplicationContext ctx = (ConfigurableApplicationContext) event.getSource();
		if (!ObjectUtils.isEmpty(this.properties.getProperties())) {
			for (PropertySource source : ctx.getEnvironment().getPropertySources()) {
				if (source instanceof EnumerablePropertySource) {
					EnumerablePropertySource e = (EnumerablePropertySource) source;
					for (String propertyName : e.getPropertyNames()) {
						for (String relaxedPropertyName : new RelaxedNames(propertyName)) {
							if (isMatch(relaxedPropertyName, this.properties.getProperties(), null)) {
								whitelistedProperties.put(propertyName, source.getProperty(propertyName));
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
