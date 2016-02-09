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
public class DefaultBinding<T> implements Binding<T> {

	private final String name;

	private final String group;

	private final T target;

	private final AbstractEndpoint endpoint;

	private final Type type;

	private final DefaultBindingPropertiesAccessor properties;

	private final AbstractBinder<T> abstractBinder;

	private DefaultBinding(String name, String group, T target, AbstractEndpoint endpoint, Type type,
						   DefaultBindingPropertiesAccessor properties, AbstractBinder<T> abstractBinder) {
		Assert.notNull(target, "target must not be null");
		Assert.notNull(endpoint, "endpoint must not be null");
		this.name = name;
		this.group = group;
		this.target = target;
		this.endpoint = endpoint;
		this.type = type;
		this.properties = properties;
		this.abstractBinder = abstractBinder;
	}


	public static <T> DefaultBinding<T> forConsumer(String name, String group, AbstractEndpoint adapterFromBinder, T inputTarget,
												  DefaultBindingPropertiesAccessor properties, AbstractBinder<T> binder) {
		return new DefaultBinding<>(name, group, inputTarget, adapterFromBinder, Binding.Type.consumer, properties, binder);
	}

	public static <T> DefaultBinding<T> forProducer(String name, T outputTarget, AbstractEndpoint adapterToBinder,
													  DefaultBindingPropertiesAccessor properties,  AbstractBinder<T> binder) {
		return new DefaultBinding<>(name, null, outputTarget, adapterToBinder, Binding.Type.producer, properties, binder);
	}


	public String getName() {
		return name;
	}

	public String getGroup() {
		return group;
	}

	@Override
	public T getTarget() {
		return target;
	}

	public AbstractEndpoint getEndpoint() {
		return endpoint;
	}

	@Override
	public Type getType() {
		return type;
	}

	@Override
	public void unbind() {
		endpoint.stop();
		abstractBinder.notifyUnbind(this);
	}

	public DefaultBindingPropertiesAccessor getPropertiesAccessor() {
		return properties;
	}

	@Override
	public String toString() {
		return type + " Binding [name=" + name + ", target=" + target + ", endpoint=" + endpoint.getComponentName()
				+ "]";
	}
}
