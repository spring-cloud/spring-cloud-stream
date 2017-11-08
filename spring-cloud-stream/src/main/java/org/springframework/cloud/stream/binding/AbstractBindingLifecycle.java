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

import org.springframework.beans.BeansException;
import org.springframework.context.SmartLifecycle;

/**
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractBindingLifecycle implements SmartLifecycle {

	private volatile boolean running;

	private final BindingService bindingService;

	private final Map<String, Bindable> bindables;

	private final boolean input;

	AbstractBindingLifecycle(BindingService bindingService, Map<String, Bindable> bindables, boolean input) {
		this.bindingService = bindingService;
		this.bindables = bindables;
		this.input = input;
	}

	@Override
	public void start() {
		if (!running) {
			for (Bindable bindable : bindables.values()) {
				if (input) {
					bindable.bindInputs(bindingService);
				}
				else {
					bindable.bindOutputs(bindingService);
				}
			}
			this.running = true;
		}
	}

	@Override
	public void stop() {
		if (running) {
			try {
				for (Bindable bindable : bindables.values()) {
					if (input) {
						bindable.unbindInputs(bindingService);
					}
					else {
						bindable.unbindOutputs(bindingService);
					}
				}
			}
			catch (BeansException e) {
				throw new IllegalStateException("Cannot perform unbinding, no proper implementation found", e);
			}
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

	/**
	 * Return a low value so that this bean is started after receiving Lifecycle beans are
	 * started. Beans that need to start before bindings will set a lower phase value.
	 */
	@Override
	public int getPhase() {
		return this.input ? Integer.MAX_VALUE - 1000 : Integer.MIN_VALUE + 1000;
	}
}
