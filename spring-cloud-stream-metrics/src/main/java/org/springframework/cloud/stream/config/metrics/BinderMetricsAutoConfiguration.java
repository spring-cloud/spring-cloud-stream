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

package org.springframework.cloud.stream.config.metrics;

import org.springframework.boot.actuate.endpoint.MetricsEndpoint;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.metrics.BinderMetricsEmitter;
import org.springframework.cloud.stream.metrics.BootMetricJsonSerializer;
import org.springframework.cloud.stream.metrics.Emitter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@ConditionalOnClass(Binder.class)
@EnableScheduling
@EnableBinding(Emitter.class)
@EnableConfigurationProperties(StreamMetricsProperties.class)
@ConditionalOnProperty("spring.cloud.stream.bindings." + Emitter.METRICS_CHANNEL_NAME + ".destination")
public class BinderMetricsAutoConfiguration {

	@Bean
	public BinderMetricsEmitter binderMetricsExporter(MetricsEndpoint endpoint) {
		return new BinderMetricsEmitter(endpoint);
	}

	@Bean
	public BootMetricJsonSerializer metricJsonSerializer() {
		return new BootMetricJsonSerializer();
	}

}
