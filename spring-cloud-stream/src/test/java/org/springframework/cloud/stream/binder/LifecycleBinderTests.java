/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class LifecycleBinderTests {

	@Test
	public void testOnlySmartLifecyclesStarted() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				TestSource.class, "--server.port=-1",
				"--spring.cloud.stream.defaultBinder=mock", "--spring.jmx.enabled=false");
		SimpleLifecycle simpleLifecycle = applicationContext
				.getBean(SimpleLifecycle.class);
		assertThat(simpleLifecycle.isRunning()).isFalse();
		applicationContext.close();
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	public static class TestSource {

		@Bean
		public SimpleLifecycle simpleLifecycle() {
			return new SimpleLifecycle();
		}

	}

	public static class SimpleLifecycle implements Lifecycle {

		private boolean running;

		@Override
		public synchronized void start() {
			this.running = true;
		}

		@Override
		public synchronized void stop() {
			this.running = false;
		}

		@Override
		public synchronized boolean isRunning() {
			return this.running;
		}

	}

}
