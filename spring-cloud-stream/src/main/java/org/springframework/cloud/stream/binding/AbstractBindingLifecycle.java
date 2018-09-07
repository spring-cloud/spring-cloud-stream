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

package org.springframework.cloud.stream.binding;

import java.util.Map;

import org.springframework.context.SmartLifecycle;

/**
 * Base implementation of lifecycle operations for {@link BindingService}
 * aware {@link Bindable}s.
 *
 * @see InputBindingLifecycle
 * @see OutputBindingLifecycle
 *
 * @author Oleg Zhurakousky
 */
abstract class AbstractBindingLifecycle implements SmartLifecycle {
	final BindingService bindingService;

	private final Map<String, Bindable> bindables;

	private volatile boolean running;

	AbstractBindingLifecycle(BindingService bindingService, Map<String, Bindable> bindables) {
		this.bindingService = bindingService;
		this.bindables = bindables;
	}

	@Override
	public void start() {
		if (!running) {
			bindables.values().forEach(this::doStartWithBindable);
			this.running = true;
		}
	}

	@Override
	public void stop() {
		if (running) {
			bindables.values().forEach(this::doStopWithBindable);
			this.running = false;
		}
	}

	@Override
	public boolean isRunning() {
		return running;
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

	abstract void doStartWithBindable(Bindable bindable);

	abstract void doStopWithBindable(Bindable bindable);
}
