/*
 * Copyright 2020-2022 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.springframework.context.Lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Myeonghyeon Lee
 *
 */
public class DefaultBinderTests {

	@Test
	void testDefaultBinderIsPaused() {
		@SuppressWarnings({ "rawtypes" })
		DefaultBinding<?> binding = new DefaultBinding<>("foo", "bar", "target", (Binding) () -> {
		});

		assertThat(binding.isPaused()).isFalse();

		binding.pause();

		assertThat(binding.isPaused()).isTrue();
	}

	@Test
	void testDefaultBinderIsNull(){
		DefaultBinding<?> binding = new DefaultBinding<>("foo", "bar", "target", null);
		assertThat(binding.getState()).isNotNull();
	}

	@Test
	void testDefaultBinderIsRunning(){
		DefaultBinding<?> binding = new DefaultBinding<>("foo", "bar", "target", new Lifecycle() {
			private Boolean running =false;
			@Override
			public void start() {
				running = !this.running;
			}

			@Override
			public void stop() {
				running = !this.running;
			}

			@Override
			public boolean isRunning() {
				return running;
			}
		});
		binding.start();
		assertThat(binding.isRunning()).isTrue();
	}
}
