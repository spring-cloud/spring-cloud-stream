/*
 * Copyright 2013-2018 the original author or authors.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.Pausable;
import org.springframework.integration.support.context.NamedComponent;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Default implementation for a {@link Binding}.
 *
 * @param <T> type of binding
 * @author Jennifer Hickey
 * @author Mark Fisher
 * @author Gary Russell
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 */
@JsonPropertyOrder({ "name", "group", "pausable", "state" })
@JsonIgnoreProperties("running")
public class DefaultBinding<T> implements Binding<T> {

	protected final String name;

	protected final String group;

	protected final T target;

	protected final Lifecycle lifecycle;

	private final Log logger = LogFactory.getLog(this.getClass().getName());

	private boolean paused;

	private boolean restartable;

	/**
	 * Creates an instance that associates a given name, group and binding target with an
	 * optional {@link Lifecycle} component, which will be stopped during unbinding.
	 * @param name the name of the binding target
	 * @param group the group (only for input targets)
	 * @param target the binding target
	 * @param lifecycle {@link Lifecycle} that runs while the binding is active and will
	 * be stopped during unbinding
	 */
	public DefaultBinding(String name, String group, T target, Lifecycle lifecycle) {
		Assert.notNull(target, "target must not be null");
		this.name = name;
		this.group = group;
		this.target = target;
		this.lifecycle = lifecycle;
		this.restartable = StringUtils.hasText(group);
	}

	public DefaultBinding(String name, T target, Lifecycle lifecycle) {
		this(name, null, target, lifecycle);
		this.restartable = true;
	}

	public String getName() {
		return this.name;
	}

	public String getGroup() {
		return this.group;
	}

	public String getState() {
		String state = "N/A";
		if (this.lifecycle != null) {
			if (isPausable()) {
				state = this.paused ? "paused" : this.getRunningState();
			}
			else {
				state = this.getRunningState();
			}
		}
		return state;
	}

	public boolean isRunning() {
		return this.lifecycle != null && this.lifecycle.isRunning();
	}

	public boolean isPausable() {
		return this.lifecycle instanceof Pausable;
	}

	@Override
	public final synchronized void start() {
		if (!this.isRunning()) {
			if (this.lifecycle != null && this.restartable) {
				this.lifecycle.start();
			}
			else {
				this.logger.warn("Can not re-bind an anonymous binding");
			}
		}
	}

	@Override
	public final synchronized void stop() {
		if (this.isRunning()) {
			this.lifecycle.stop();
		}
	}

	@Override
	public final synchronized void pause() {
		if (this.lifecycle instanceof Pausable) {
			((Pausable) this.lifecycle).pause();
			this.paused = true;
		}
		else {
			this.logger
					.warn("Attempted to pause a component that does not support Pausable "
							+ this.lifecycle);
		}
	}

	@Override
	public final synchronized void resume() {
		if (this.lifecycle instanceof Pausable) {
			((Pausable) this.lifecycle).resume();
			this.paused = false;
		}
		else {
			this.logger.warn(
					"Attempted to resume a component that does not support Pausable "
							+ this.lifecycle);
		}
	}

	@Override
	public final void unbind() {
		this.stop();
		afterUnbind();
	}

	Lifecycle getEndpoint() {
		return this.lifecycle;
	}

	@Override
	public String toString() {
		return " Binding [name=" + this.name + ", target=" + this.target + ", lifecycle="
				+ ((this.lifecycle instanceof NamedComponent)
						? ((NamedComponent) this.lifecycle).getComponentName()
						: ObjectUtils.nullSafeToString(this.lifecycle))
				+ "]";
	}

	/**
	 * Listener method that executes after unbinding. Subclasses can implement their own
	 * behaviour on unbinding by overriding this method.
	 */
	protected void afterUnbind() {
	}

	private String getRunningState() {
		return isRunning() ? "running" : "stopped";
	}

}
