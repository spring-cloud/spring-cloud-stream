/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.function;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BoundTargetHolder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for creating inputs/outputs destinations to be bound to
 * function arguments. It is an extension to {@link BindableProxyFactory} which
 * operates on Bindable interfaces (e.g., Source, Processor, Sink) which internally
 * define inputs and output channels. Unlike BindableProxyFactory, this class simply
 * operates based on the count of provided inputs and outputs and the names of inputs and outputs
 * are based on convention.
 *
 * @author Oleg Zhurakousky
 *
 *
 *
 * @since 3.0
 */
public class BindableFunctionProxyFactory extends BindableProxyFactory {

	private final int inputCount;

	private final int outputCount;

	public BindableFunctionProxyFactory(int inputCount, int outputCount) {
		super(null);
		this.inputCount = inputCount;
		this.outputCount = outputCount;
	}


	@Override
	public void afterPropertiesSet() {
		Assert.notEmpty(BindableFunctionProxyFactory.this.bindingTargetFactories,
				"'bindingTargetFactories' cannot be empty");

		if (this.inputCount > 0) {
			if (this.inputCount == 1) {
				this.createInput("input");
			}
			else {
				throw new UnsupportedOperationException("Multiple inputs are not currently supported");
			}
		}

		if (this.outputCount > 0) {
			if (this.outputCount == 1) {
				this.createOutput("output");
			}
			else {
				throw new UnsupportedOperationException("Multiple outputs are not currently supported");
			}
		}
	}

	private void createInput(String name) {
		BindableFunctionProxyFactory.this.inputHolders.put(name,
				new BoundTargetHolder(getBindingTargetFactory(SubscribableChannel.class)
						.createInput(name), true));
	}

	private void createOutput(String name) {
		BindableFunctionProxyFactory.this.outputHolders.put(name,
				new BoundTargetHolder(getBindingTargetFactory(MessageChannel.class)
						.createOutput(name), true));
	}


	@Override
	public Class<?> getObjectType() {
		return this.type;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
