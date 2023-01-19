/*
 * Copyright 2019-2021 the original author or authors.
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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BoundTargetHolder;
import org.springframework.cloud.stream.binding.SupportedBindableFeatures;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * {@link FactoryBean} for creating inputs/outputs destinations to be bound to
 * function arguments. It is an extension to {@link BindableProxyFactory} which
 * operates on Bindable interfaces (e.g., Source, Processor, Sink) which internally
 * define inputs and output channels. Unlike BindableProxyFactory, this class
 * operates based on the count of provided inputs and outputs deriving the binding
 * (channel) names based on convention - {@code `<function-definition>. + <in/out> + .<index>`}
 * <br>
 * For example, `myFunction.in.0` - is the binding for the first input argument of the
 * function with the name `myFunction`.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 * @since 3.0
 */
public class BindableFunctionProxyFactory extends BindableProxyFactory implements ApplicationContextAware {

	private final int inputCount;

	private final int outputCount;

	private final String functionDefinition;

	private final StreamFunctionProperties functionProperties;

	private final SupportedBindableFeatures supportedBindableFeatures;

	private final boolean functionExist;

	private GenericApplicationContext context;

	BindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties) {
		this(functionDefinition, inputCount, outputCount, functionProperties, new SupportedBindableFeatures(), true);
	}

	BindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties, boolean functionExist) {
		this(functionDefinition, inputCount, outputCount, functionProperties, new SupportedBindableFeatures(), functionExist);
	}

	BindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties, SupportedBindableFeatures supportedBindableFeatures) {
		this(functionDefinition, inputCount, outputCount, functionProperties, supportedBindableFeatures, true);
	}

	BindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties,
			SupportedBindableFeatures supportedBindableFeatures, boolean functionExist) {
		super(null);
		this.inputCount = inputCount;
		this.outputCount = outputCount;
		this.functionDefinition = functionDefinition;
		this.functionProperties = functionProperties;
		this.supportedBindableFeatures = supportedBindableFeatures;
		this.functionExist = functionExist;
	}

	@Override
	public void afterPropertiesSet() {
		populateBindingTargetFactories(beanFactory);
		Assert.notEmpty(BindableFunctionProxyFactory.this.bindingTargetFactories,
				"'bindingTargetFactories' cannot be empty");

		if (this.inputCount > 0) {
			for (int i = 0; i < inputCount; i++) {
				this.createInput(this.buildInputNameForIndex(i));
			}
		}

		if (this.outputCount > 0) {
			for (int i = 0; i < outputCount; i++) {
				this.createOutput(this.buildOutputNameForIndex(i));
			}
		}
	}

	@Override
	public Class<?> getObjectType() {
		return this.type;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	protected String getFunctionDefinition() {
		return this.isFunctionExist() ? this.functionDefinition : null;
	}

	protected String getInputName(int index) {
		return CollectionUtils.isEmpty(this.getInputs())
				? null
						: this.getInputs().toArray(new String[0])[index];
	}

	protected String getOutputName(int index) {
		String outputName = null;
		if (this.outputCount > 0) {
			outputName = CollectionUtils.isEmpty(this.getOutputs())
					? null
							: this.getOutputs().toArray(new String[0])[index];
		}
		return outputName;
	}

	protected boolean isMultiple() {
		return  this.inputCount > 1 || this.outputCount > 1;
	}

	private String buildInputNameForIndex(int index) {
		if (!this.isFunctionExist()) {
			return this.functionDefinition;
		}
		return new StringBuilder(this.functionDefinition.replace(",", "|").replace("|", ""))
			.append(FunctionConstants.DELIMITER)
			.append(FunctionConstants.DEFAULT_INPUT_SUFFIX)
			.append(FunctionConstants.DELIMITER)
			.append(index)
			.toString();
	}

	private String buildOutputNameForIndex(int index) {
		if (!this.isFunctionExist()) {
			return this.functionDefinition;
		}
		return new StringBuilder(this.functionDefinition.replace(",", "|").replace("|", ""))
				.append(FunctionConstants.DELIMITER)
				.append(FunctionConstants.DEFAULT_OUTPUT_SUFFIX)
				.append(FunctionConstants.DELIMITER)
				.append(index)
				.toString();
	}

	private void createInput(String name) {
		if (this.functionProperties.getBindings().containsKey(name)) {
			name = this.functionProperties.getBindings().get(name);
		}
		if (this.supportedBindableFeatures.isPollable()) {
			PollableMessageSource pollableSource = (PollableMessageSource) getBindingTargetFactory(PollableMessageSource.class).createInput(name);
			if (context != null && !context.containsBean(name)) {
				context.registerBean(name, PollableMessageSource.class, () -> pollableSource);
			}
			this.inputHolders.put(name, new BoundTargetHolder(pollableSource, true));
		}
		else if (this.supportedBindableFeatures.isReactive()) {
			this.inputHolders.put(name,
				new BoundTargetHolder(getBindingTargetFactory(FluxMessageChannel.class)
					.createInput(name), true));
		}
		else {
			this.inputHolders.put(name,
					new BoundTargetHolder(getBindingTargetFactory(SubscribableChannel.class)
							.createInput(name), true));
		}
	}

	private void createOutput(String name) {
		if (this.functionProperties.getBindings().containsKey(name)) {
			name = this.functionProperties.getBindings().get(name);
		}
		if (this.supportedBindableFeatures.isReactive()) {
			this.outputHolders.put(name,
				new BoundTargetHolder(getBindingTargetFactory(FluxMessageChannel.class)
					.createOutput(name), true));
		}
		else {
			this.outputHolders.put(name,
				new BoundTargetHolder(getBindingTargetFactory(SubscribableChannel.class)
					.createOutput(name), true));
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = (GenericApplicationContext) applicationContext;
	}

	public boolean isFunctionExist() {
		return functionExist;
	}
}
