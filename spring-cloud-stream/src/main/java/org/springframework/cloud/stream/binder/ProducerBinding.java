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

package org.springframework.cloud.stream.binder;

import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;

/**
 * An implementation of {@link DefaultBinding} that uses {@link ProducerDestination} as a source of configuration
 * @author Vinicius Carvalho
 */
public class ProducerBinding<T> extends DefaultBinding<T,ProducerDestination> {
	/**
	 * Creates an instance that associates a given name, group and binding target with an
	 * optional {@link Lifecycle} component, which will be stopped during unbinding.
	 *  @param name the name of the binding target
	 * @param group the group (only for input targets)
	 * @param target the binding target
	 * @param lifecycle {@link Lifecycle} that runs while the binding is active and will
	 * @param provisioningDestination the provisioned destination created for this binding
	 */
	public ProducerBinding(String name, String group, T target, Lifecycle lifecycle, ProducerDestination provisioningDestination) {
		super(name, group, target, lifecycle, provisioningDestination);
	}
}
