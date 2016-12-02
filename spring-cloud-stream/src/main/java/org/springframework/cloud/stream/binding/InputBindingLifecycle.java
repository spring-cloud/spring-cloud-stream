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

package org.springframework.cloud.stream.binding;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;

/**
 * Coordinates binding/unbinding of input binding targets in accordance to the lifecycle
 * of the host context.
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class InputBindingLifecycle implements SmartLifecycle, ApplicationContextAware {

	private volatile boolean running;

	private ConfigurableApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public void start() {
		if (!running) {
			// retrieve the BindingService lazily, avoiding early initialization
			try {
				BindingService bindingService = this.applicationContext
						.getBean(BindingService.class);
				Map<String, Bindable> bindables = this.applicationContext
						.getBeansOfType(Bindable.class);
				for (Bindable bindable : bindables.values()) {
					bindable.bindInputs(bindingService);
				}
			}
			catch (BeansException e) {
				throw new IllegalStateException(
						"Cannot perform binding, no proper implementation found", e);
			}
			this.running = true;
		}
	}

	@Override
	public void stop() {
		if (running) {
			try {
				// retrieve the BindingService lazily, avoiding early
				// initialization
				BindingService bindingService = this.applicationContext
						.getBean(BindingService.class);
				Map<String, Bindable> bindables = this.applicationContext
						.getBeansOfType(Bindable.class);
				for (Bindable bindable : bindables.values()) {
					bindable.unbindInputs(bindingService);
				}
			}
			catch (BeansException e) {
				throw new IllegalStateException(
						"Cannot perform unbinding, no proper implementation found", e);
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
	 * Return a high value so that this bean is started after receiving Lifecycle beans
	 * are started. Beans that need to start after bindings will set a higher phase value.
	 */
	@Override
	public int getPhase() {
		return Integer.MAX_VALUE - 1000;
	}
}
