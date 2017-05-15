/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.actuate.metrics.reader.MetricReader;
import org.springframework.integration.monitor.IntegrationMBeanExporter;
import org.springframework.integration.support.management.Statistics;
import org.springframework.util.Assert;

/**
 * A customized version of
 * {@link org.springframework.boot.actuate.metrics.integration.SpringIntegrationMetricReader}
 * that provides support for customizing channels with a namespace prefix.
 *
 * @author Marius Bogoevici
 * @see org.springframework.boot.actuate.metrics.integration.SpringIntegrationMetricReader
 * for original implementation
 */
public class NamespaceAwareSpringIntegrationMetricReader implements MetricReader {

	private final String namespace;

	private final IntegrationMBeanExporter exporter;

	public NamespaceAwareSpringIntegrationMetricReader(String namespace, IntegrationMBeanExporter exporter) {
		Assert.hasText(namespace, "cannot be null or empty String");
		Assert.notNull(exporter, "cannot be null");
		this.namespace = namespace;
		this.exporter = exporter;
	}

	@Override
	public Metric<?> findOne(String metricName) {
		return null;
	}

	@Override
	public Iterable<Metric<?>> findAll() {
		IntegrationMBeanExporter exporter = this.exporter;
		List<Metric<?>> metrics = new ArrayList<Metric<?>>();
		for (String name : exporter.getChannelNames()) {
			String prefix = "integration.channel." + namespace + "." + name;
			metrics.addAll(getStatistics(prefix + ".errorRate",
					exporter.getChannelErrorRate(name)));
			metrics.add(new Metric<Long>(prefix + ".sendCount",
					exporter.getChannelSendCountLong(name)));
			metrics.addAll(getStatistics(prefix + ".sendRate",
					exporter.getChannelSendRate(name)));
			metrics.add(new Metric<Long>(prefix + ".receiveCount",
					exporter.getChannelReceiveCountLong(name)));
		}
		for (String name : exporter.getHandlerNames()) {
			metrics.addAll(getStatistics("integration." + namespace + ".handler." + name + ".duration",
					exporter.getHandlerDuration(name)));
		}
		metrics.add(new Metric<Integer>("integration." + namespace + ".activeHandlerCount",
				exporter.getActiveHandlerCount()));
		metrics.add(new Metric<Integer>("integration." + namespace + ".handlerCount",
				exporter.getHandlerCount()));
		metrics.add(new Metric<Integer>("integration." + namespace + ".channelCount",
				exporter.getChannelCount()));
		metrics.add(new Metric<Integer>("integration." + namespace + ".queuedMessageCount",
				exporter.getQueuedMessageCount()));
		return metrics;
	}

	private Collection<? extends Metric<?>> getStatistics(String name,
			Statistics statistic) {
		List<Metric<?>> metrics = new ArrayList<Metric<?>>();
		metrics.add(new Metric<Double>(name + ".mean", statistic.getMean()));
		metrics.add(new Metric<Double>(name + ".max", statistic.getMax()));
		metrics.add(new Metric<Double>(name + ".min", statistic.getMin()));
		metrics.add(
				new Metric<Double>(name + ".stdev", statistic.getStandardDeviation()));
		metrics.add(new Metric<Long>(name + ".count", statistic.getCountLong()));
		return metrics;
	}

	@Override
	public long count() {
		int totalChannelCount = this.exporter.getChannelCount() * 11;
		int totalHandlerCount = this.exporter.getHandlerCount() * 5;
		return totalChannelCount + totalHandlerCount + 4;
	}

}
