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

package org.springframework.cloud.stream.binder.kstream;

import java.util.Set;

import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;

/**
 * @author Soby Chacko
 */
public class StreamsBuildersLifecycle implements SmartLifecycle {

	private final KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue;
	private final QueryableStoreRegistry queryableStoreRegistry;

	private volatile boolean running;

	public StreamsBuildersLifecycle(KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue, QueryableStoreRegistry queryableStoreRegistry) {
		this.kStreamBindingInformationCatalogue = kStreamBindingInformationCatalogue;
		this.queryableStoreRegistry = queryableStoreRegistry;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans = this.kStreamBindingInformationCatalogue.getStreamsBuilderFactoryBeans();
				for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
					streamsBuilderFactoryBean.start();
					queryableStoreRegistry.registerKafkaStreams(streamsBuilderFactoryBean.getKafkaStreams());
				}
				this.running = true;
			} catch (Exception e) {
				throw new KafkaException("Could not start stream: ", e);
			}
		}
	}

	@Override
	public synchronized  void stop() {
			if (this.running) {
				try {
					Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans = this.kStreamBindingInformationCatalogue.getStreamsBuilderFactoryBeans();
					for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
						streamsBuilderFactoryBean.stop();
					}
				}
				catch (Exception e) {
					throw new IllegalStateException(e);
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
