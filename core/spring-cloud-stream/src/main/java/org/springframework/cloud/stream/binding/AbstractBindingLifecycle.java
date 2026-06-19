/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.BindableFunctionProxyFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;

/**
 * Base implementation of lifecycle operations for {@link BindingService} aware
 * {@link Bindable}s.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 * @see InputBindingLifecycle
 * @see OutputBindingLifecycle
 */
public abstract class AbstractBindingLifecycle implements SmartLifecycle {

	final BindingService bindingService;

	final Map<String, Bindable> bindables;

	@Autowired
	private ApplicationContext context;

	private volatile boolean running;

	AbstractBindingLifecycle(BindingService bindingService,
			Map<String, Bindable> bindables) {
		this.bindingService = bindingService;
		this.bindables = bindables;
	}

	@Override
	public void start() {
		if (!this.running) {
			if (this.context != null) {
				this.bindables.putAll(context.getBeansOfType(Bindable.class));
			}

			List<Bindable> startedBindables = new ArrayList<>();
			try {
				for (Bindable bindable : this.bindables.values()) {
					startedBindables.add(bindable);
					this.doStartWithBindable(bindable);
				}
			}
			catch (RuntimeException ex) {
				this.stopStartedBindables(startedBindables, ex);
				throw ex;
			}
			this.running = true;
		}
	}

	@Override
	public void stop() {
		if (this.running) {
			if (this.context != null) {
				this.bindables.putAll(context.getBeansOfType(Bindable.class));
			}
			this.bindables.values().forEach(this::doStopWithBindable);
			this.running = false;
		}
	}

	@Override
	public boolean isRunning() {
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

	public void startBindable(Bindable bindable) {
		if (bindable instanceof BindableFunctionProxyFactory functionProxy) {
			this.bindables.put(functionProxy.getFunctionDefinition(), bindable);
		}
		try {
			this.doStartWithBindable(bindable);
		}
		catch (RuntimeException ex) {
			this.stopStartedBindables(List.of(bindable), ex);
			throw ex;
		}
	}

	private void stopStartedBindables(List<Bindable> startedBindables,
			RuntimeException exception) {
		for (int i = startedBindables.size() - 1; i >= 0; i--) {
			try {
				this.doStopWithBindable(startedBindables.get(i));
			}
			catch (RuntimeException stopException) {
				exception.addSuppressed(stopException);
			}
		}
	}

	abstract void doStartWithBindable(Bindable bindable);

	abstract void doStopWithBindable(Bindable bindable);

}
