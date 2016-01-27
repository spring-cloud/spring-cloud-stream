/*
 * Copyright 2013-2016 the original author or authors.
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

import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.util.Assert;

/**
 * Represents a binding between an input or output and an adapter endpoint that connects via a Binder. The binding
 * could be for a consumer or a producer. A consumer binding represents a connection from an adapter to an
 * input. A producer binding represents a connection from an output to an adapter.
 *
 * @author Jennifer Hickey
 * @author Mark Fisher
 * @author Gary Russell
 * @author Marius Bogoevici
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 */
public class Binding<T> implements Lifecycle {

	public enum Type {
		producer, consumer
	}

	private final String name;

	private final String group;

	private final T target;

	private final AbstractEndpoint endpoint;

	private final Type type;

	private final DefaultBindingPropertiesAccessor properties;

	private Binding(String name, String group, T target, AbstractEndpoint endpoint, Type type,
			DefaultBindingPropertiesAccessor properties) {
		Assert.notNull(target, "target must not be null");
		Assert.notNull(endpoint, "endpoint must not be null");
		this.name = name;
		this.group = group;
		this.target = target;
		this.endpoint = endpoint;
		this.type = type;
		this.properties = properties;
	}

	public static <T> Binding<T> forConsumer(String name, String group, AbstractEndpoint adapterFromBinder, T inputTarget,
			DefaultBindingPropertiesAccessor properties) {
		return new Binding<>(name, group, inputTarget, adapterFromBinder, Type.consumer, properties);
	}

	public static <T> Binding<T> forProducer(String name, T outputTarget, AbstractEndpoint adapterToBinder,
			DefaultBindingPropertiesAccessor properties) {
		return new Binding<>(name, null, outputTarget, adapterToBinder, Type.producer, properties);
	}

	public String getName() {
		return name;
	}

	public String getGroup() {
		return group;
	}

	public T getTarget() {
		return target;
	}

	public AbstractEndpoint getEndpoint() {
		return endpoint;
	}

	public Type getType() {
		return type;
	}

	public DefaultBindingPropertiesAccessor getPropertiesAccessor() {
		return properties;
	}

	@Override
	public void start() {
		endpoint.start();
	}

	@Override
	public void stop() {
		endpoint.stop();
	}

	@Override
	public boolean isRunning() {
		return endpoint.isRunning();
	}

	@Override
	public String toString() {
		return type + " Binding [name=" + name + ", target=" + target + ", endpoint=" + endpoint.getComponentName()
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Binding<?> other = (Binding<?>) obj;
		if (endpoint == null) {
			if (other.endpoint != null)
				return false;
		} else if (!endpoint.equals(other.endpoint))
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equals(other.target))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

}
