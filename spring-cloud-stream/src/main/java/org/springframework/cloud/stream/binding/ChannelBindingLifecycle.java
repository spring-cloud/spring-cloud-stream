/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
 * Coordinates binding/unbinding of input/output channels in accordance to the lifecycle of the host context.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ChannelBindingLifecycle implements SmartLifecycle, ApplicationContextAware {

	private volatile boolean running = false;

	private final Object lifecycleMonitor = new Object();

	private ConfigurableApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public void start() {
		if (!running) {
			synchronized (lifecycleMonitor) {
				if (!running) {
					// retrieve the ChannelBindingService lazily, avoiding early initialization
					try {
						ChannelBindingService channelBindingService = this.applicationContext.getBean(ChannelBindingService.class);
						Map<String, Bindable> bindables = this.applicationContext.getBeansOfType(Bindable.class);
						for (Bindable bindable : bindables.values()) {
							bindable.bindOutputs(channelBindingService);
						}
						for (Bindable bindable : bindables.values()) {
							bindable.bindInputs(channelBindingService);
						}
					} catch (BeansException e) {
						throw new IllegalStateException("Cannot perform binding, no proper implementation found",e);
					}
					this.running = true;
				}
			}
		}
	}

	@Override
	public void stop() {
		if (running) {
			synchronized (lifecycleMonitor) {
				if (running) {
					try {
						// retrieve the ChannelBindingService lazily, avoiding early initialization
						ChannelBindingService channelBindingService = this.applicationContext.getBean(ChannelBindingService.class);
						Map<String, Bindable> bindables = this.applicationContext.getBeansOfType(Bindable.class);
						for (Bindable bindable : bindables.values()) {
							bindable.unbindInputs(channelBindingService);
						}
						for (Bindable bindable : bindables.values()) {
							bindable.unbindOutputs(channelBindingService);
						}
					} catch (BeansException e) {
						throw new IllegalStateException("Cannot perform binding, no proper implementation found",e);
					}
					this.running = false;
				}
			}
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
	 * Return the lowest value to start this bean before any message producing lifecycle
	 * beans.
	 */
	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}
}
