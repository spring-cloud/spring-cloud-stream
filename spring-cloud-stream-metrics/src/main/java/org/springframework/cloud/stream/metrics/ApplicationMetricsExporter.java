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

import org.springframework.boot.actuate.endpoint.MetricsEndpoint;
import org.springframework.boot.actuate.endpoint.MetricsEndpointMetricReader;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.actuate.metrics.export.Exporter;
import org.springframework.boot.actuate.metrics.export.MetricCopyExporter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 *
 * Component that sends {@link ApplicationMetrics} from
 * {@link MetricsEndpointMetricReader} downstream via the configured metrics channel.
 *
 * It uses the Spring Boot support for {@link Exporter} to periodically emit messages
 * polled from the endpoint.
 *
 * @author Vinicius Carvalho
 */
public class ApplicationMetricsExporter implements Exporter {

	private MessageChannel source;

	private ApplicationMetricsProperties properties;

	private MetricsEndpointMetricReader metricsReader;

	public ApplicationMetricsExporter(MetricsEndpoint endpoint, MessageChannel source,
			ApplicationMetricsProperties properties) {
		this.source = source;
		this.properties = properties;
		this.metricsReader = new MetricsEndpointMetricReader(endpoint);
	}

	@Override
	public void export() {
		ApplicationMetrics appMetrics = new ApplicationMetrics(
				this.properties.getMetricName(),
				filter());
		appMetrics.setProperties(this.properties.getExportProperties());
		source.send(MessageBuilder.withPayload(appMetrics).build());
	}

	/**
	 * Copy of similarly named method in {@link MetricCopyExporter}.
	 */
	protected Collection<Metric<?>> filter() {
		Collection<Metric<?>> result = new ArrayList<>();
		Iterable<Metric<?>> metrics = metricsReader.findAll();
		for (Metric<?> metric : metrics) {
			if (isMatch(metric.getName(), this.properties.getTrigger().getIncludes(),
					this.properties.getTrigger().getExcludes())) {
				result.add(metric);
			}
		}
		return result;
	}

	/**
	 * Copy of similarly named method in {@link MetricCopyExporter}.
	 */
	private boolean isMatch(String name, String[] includes, String[] excludes) {
		if (ObjectUtils.isEmpty(includes)
				|| PatternMatchUtils.simpleMatch(includes, name)) {
			return !PatternMatchUtils.simpleMatch(excludes, name);
		}
		return false;
	}

}
