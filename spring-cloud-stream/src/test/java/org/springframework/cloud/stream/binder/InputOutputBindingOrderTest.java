/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.config.BinderFactoryConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class InputOutputBindingOrderTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testInputOutputBindingOrder() {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TestSource.class, "--server.port=-1");
		@SuppressWarnings("rawtypes")
		Binder binder = applicationContext.getBean(BinderFactory.class).getBinder(null);
		Processor processor = applicationContext.getBean(Processor.class);
		// input is bound after the context has been started
		verify(binder).bindConsumer(eq("input"), anyString(), eq(processor.input()), Mockito.<ConsumerProperties>any());
		SomeLifecycle someLifecycle = applicationContext.getBean(SomeLifecycle.class);
		assertThat(someLifecycle.isRunning());
		applicationContext.close();
		assertThat(someLifecycle.isRunning()).isFalse();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@Import({MockBinderRegistryConfiguration.class, BinderFactoryConfiguration.class})
	public static class TestSource {

		@Bean
		public SomeLifecycle someLifecycle() {
			return new SomeLifecycle();
		}
	}

	public static class SomeLifecycle implements SmartLifecycle {

		@SuppressWarnings("rawtypes")
		@Autowired
		private BinderFactory binderFactory;

		@Autowired
		private Processor processor;

		private boolean running;

		@Override
		@SuppressWarnings("unchecked")
		public synchronized void start() {
			Binder binder = this.binderFactory.getBinder(null);
			verify(binder).bindProducer(eq("output"), eq(this.processor.output()), Mockito.<ProducerProperties>any());
			// input was not bound yet
			verifyNoMoreInteractions(binder);
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
		public int getPhase() {
			return 0;
		}
	}
}
