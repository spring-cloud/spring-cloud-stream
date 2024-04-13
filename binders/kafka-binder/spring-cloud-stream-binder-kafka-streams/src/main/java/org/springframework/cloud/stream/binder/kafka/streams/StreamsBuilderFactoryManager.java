/*
 * Copyright 2018-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;
import org.springframework.lang.NonNull;

/**
 * Iterate through all {@link StreamsBuilderFactoryBean} in the application context and
 * start them. As each one completes starting, register the associated KafkaStreams object
 * into {@link InteractiveQueryService}.
 *
 * This {@link SmartLifecycle} class ensures that the bean created from it is started very
 * late through the bootstrap process by setting the phase value closer to
 * Integer.MAX_VALUE. This is to guarantee that the {@link StreamsBuilderFactoryBean} on a
 * function with multiple bindings is only started after all the binding phases have completed successfully.
 *
 * @author Soby Chacko
 */
public class StreamsBuilderFactoryManager implements SmartLifecycle {

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	private final KafkaStreamsBinderMetrics kafkaStreamsBinderMetrics;

	private final KafkaStreamsMicrometerListener listener;

	private volatile boolean running;

	private final KafkaProperties kafkaProperties;

	StreamsBuilderFactoryManager(KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
								KafkaStreamsRegistry kafkaStreamsRegistry,
								KafkaStreamsBinderMetrics kafkaStreamsBinderMetrics,
								KafkaStreamsMicrometerListener listener,
								KafkaProperties kafkaProperties) {
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
		this.kafkaStreamsBinderMetrics = kafkaStreamsBinderMetrics;
		this.listener = listener;
		this.kafkaProperties = kafkaProperties;
	}

	@Override
	public boolean isAutoStartup() {
		return this.kafkaProperties == null || this.kafkaProperties.getStreams().isAutoStartup();
	}

	@Override
	public void stop(@NonNull Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans = this.kafkaStreamsBindingInformationCatalogue
						.getStreamsBuilderFactoryBeans();
				for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
					if (this.listener != null) {
						streamsBuilderFactoryBean.addListener(this.listener);
					}
					// By default, we shut down the client if there is an uncaught exception in the application.
					// Users can override this by customizing SBFB. See this issue for more details:
					// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1110
					streamsBuilderFactoryBean.setStreamsUncaughtExceptionHandler(exception ->
							StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
					// Starting the stream.
					final Map<StreamsBuilderFactoryBean, List<ConsumerProperties>> bindingServicePropertiesPerSbfb =
							this.kafkaStreamsBindingInformationCatalogue.getConsumerPropertiesPerSbfb();
					final List<ConsumerProperties> consumerProperties = bindingServicePropertiesPerSbfb.get(streamsBuilderFactoryBean);
					final boolean autoStartupDisabledOnAtLeastOneConsumerBinding = consumerProperties.stream().anyMatch(consumerProperties1 -> !consumerProperties1.isAutoStartup());
					if (streamsBuilderFactoryBean instanceof SmartInitializingSingleton) {
						((SmartInitializingSingleton) streamsBuilderFactoryBean).afterSingletonsInstantiated();
					}
					if (!autoStartupDisabledOnAtLeastOneConsumerBinding) {
						streamsBuilderFactoryBean.start();
						this.kafkaStreamsRegistry.registerKafkaStreams(streamsBuilderFactoryBean);
					}
				}
				if (this.kafkaStreamsBinderMetrics != null) {
					this.kafkaStreamsBinderMetrics.addMetrics(streamsBuilderFactoryBeans);
				}
				this.running = true;
			}
			catch (Exception ex) {
				throw new KafkaException("Could not start stream: ", ex);
			}
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			try {
				Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans = this.kafkaStreamsBindingInformationCatalogue
						.getStreamsBuilderFactoryBeans();
				int n = 0;
				for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
					streamsBuilderFactoryBean.removeListener(this.listener);
					streamsBuilderFactoryBean.stop();
				}
				for (ProducerFactory<byte[], byte[]> dlqProducerFactory : this.kafkaStreamsBindingInformationCatalogue.getDlqProducerFactories()) {
					((DisposableBean) dlqProducerFactory).destroy();
				}
			}
			catch (Exception ex) {
				throw new IllegalStateException(ex);
			}
			finally {
				this.running = false;
			}
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE - 100;
	}

}
