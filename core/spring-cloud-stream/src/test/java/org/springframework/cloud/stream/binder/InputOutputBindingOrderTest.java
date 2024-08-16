/*
 * Copyright 2015-2024 the original author or authors.
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

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 * @author Soby Chacko
 */
class InputOutputBindingOrderTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void inputOutputBindingOrder() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				TestSource.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.defaultBinder=mock",
						"--spring.jmx.enabled=false");
		Binder binder = applicationContext.getBean(BinderFactory.class).getBinder(null,
				MessageChannel.class);
		// input is bound after the context has been started
		verify(binder).bindConsumer(eq("processor-in-0"), isNull(), Mockito.any(MessageChannel.class),
				Mockito.any());
		SomeLifecycle someLifecycle = applicationContext.getBean(SomeLifecycle.class);
		assertThat(someLifecycle.isRunning()).isTrue();
		applicationContext.close();
		assertThat(someLifecycle.isRunning()).isFalse();
		applicationContext.close();
	}

	@EnableAutoConfiguration
	public static class TestSource {

		@Bean
		public SomeLifecycle someLifecycle() {
			return new SomeLifecycle();
		}

		@Bean
		public Function<String, String> processor() {
			return s -> s;
		}

	}

	public static class SomeLifecycle implements SmartLifecycle {

		@Autowired
		private BinderFactory binderFactory;

		private boolean running;

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public synchronized void start() {
			Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
			verify(binder).bindProducer(eq("processor-out-0"), Mockito.any(MessageChannel.class),
					Mockito.any());
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
