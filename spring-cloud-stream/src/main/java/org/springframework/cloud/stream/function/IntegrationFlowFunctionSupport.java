/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.FluxSupplier;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @since 2.1
 */
public class IntegrationFlowFunctionSupport {

	private final FunctionCatalog functionCatalog;

	private final FunctionInspector functionInspector;

	private final CompositeMessageConverterFactory messageConverterFactory;

	private final StreamFunctionProperties functionProperties;

	@Autowired
	private MessageChannel errorChannel;

	IntegrationFlowFunctionSupport(FunctionCatalog functionCatalog,
			FunctionInspector functionInspector,
			CompositeMessageConverterFactory messageConverterFactory,
			StreamFunctionProperties functionProperties,
			BindingServiceProperties bindingServiceProperties) {
		Assert.notNull(functionCatalog, "'functionCatalog' must not be null");
		Assert.notNull(functionInspector, "'functionInspector' must not be null");
		Assert.notNull(messageConverterFactory,
				"'messageConverterFactory' must not be null");
		Assert.notNull(functionProperties, "'functionProperties' must not be null");
		this.functionCatalog = functionCatalog;
		this.functionInspector = functionInspector;
		this.messageConverterFactory = messageConverterFactory;
		this.functionProperties = functionProperties;
		this.functionProperties.setBindingServiceProperties(bindingServiceProperties);
	}

	/**
	 * Determines if function specified via 'spring.cloud.stream.function.definition'
	 * property can be located in {@link FunctionCatalog}s.
	 * @param <T> type of function
	 * @param typeOfFunction must be Supplier, Function or Consumer
	 * @return {@code true} if function is already stored
	 */
	public <T> boolean containsFunction(Class<T> typeOfFunction) {
		return StringUtils.hasText(this.functionProperties.getDefinition())
				&& this.catalogContains(typeOfFunction,
						this.functionProperties.getDefinition());
	}

	/**
	 * Determines if function specified via 'spring.cloud.stream.function.definition'
	 * property can be located in {@link FunctionCatalog}.
	 * @param <T> type of function
	 * @param typeOfFunction must be Supplier, Function or Consumer
	 * @param functionName the function name to check
	 * @return {@code true} if function is already stored
	 */
	public <T> boolean containsFunction(Class<T> typeOfFunction, String functionName) {
		return StringUtils.hasText(functionName)
				&& this.catalogContains(typeOfFunction, functionName);
	}

	/**
	 * @return the function type basing on the function definition.
	 */
	public FunctionType getCurrentFunctionType() {
		FunctionType functionType = this.functionInspector.getRegistration(
				this.functionCatalog.lookup(this.functionProperties.getDefinition()))
				.getType();
		return functionType;
	}

	/**
	 * Create an instance of the {@link IntegrationFlowBuilder} from a {@link Supplier}
	 * bean available in the context. The name of the bean must be provided via
	 * `spring.cloud.stream.function.definition` property.
	 * @return instance of {@link IntegrationFlowBuilder}
	 * @throws IllegalStateException if the named Supplier can not be located.
	 */
	public IntegrationFlowBuilder integrationFlowFromNamedSupplier() {
		if (StringUtils.hasText(this.functionProperties.getDefinition())) {
			Supplier<?> supplier = this.functionCatalog.lookup(Supplier.class,
					this.functionProperties.getDefinition());
			if (supplier instanceof FluxSupplier) {
				supplier = ((FluxSupplier<?>) supplier).getTarget();
			}
			return integrationFlowFromProvidedSupplier(supplier).split();
		}

		throw new IllegalStateException(
				"A Supplier is not specified in the 'spring.cloud.stream.function.definition' property.");
	}

	/**
	 * Create an instance of the {@link IntegrationFlowBuilder} from a provided
	 * {@link Supplier}.
	 * @param supplier supplier from which the flow builder will be built
	 * @return instance of {@link IntegrationFlowBuilder}
	 */
	public IntegrationFlowBuilder integrationFlowFromProvidedSupplier(
			Supplier<?> supplier) {
		return IntegrationFlows.from(supplier);
	}

	/**
	 * @param inputChannel channel for which flow we be built
	 * @return instance of {@link IntegrationFlowBuilder}
	 */
	public IntegrationFlowBuilder integrationFlowFromChannel(
			SubscribableChannel inputChannel) {
		IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(inputChannel);
		return flowBuilder;
	}

	/**
	 * @param inputChannel channel for which flow we be built
	 * @param outputChannel channel for which flow we be built
	 * @return instance of {@link IntegrationFlowBuilder}
	 */
	public IntegrationFlowBuilder integrationFlowForFunction(
			SubscribableChannel inputChannel, MessageChannel outputChannel) {

		if (inputChannel instanceof IntegrationObjectSupport) {
			String inputBindingName = ((IntegrationObjectSupport) inputChannel)
					.getComponentName();
			if (StringUtils.hasText(inputBindingName)) {
				this.functionProperties.setInputDestinationName(inputBindingName);
			}
		}

		if (outputChannel instanceof IntegrationObjectSupport) {
			String outputBindingName = ((IntegrationObjectSupport) outputChannel)
					.getComponentName();
			if (StringUtils.hasText(outputBindingName)) {
				this.functionProperties.setOutputDestinationName(outputBindingName);
			}
		}

		IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(inputChannel);

		if (!this.andThenFunction(flowBuilder, outputChannel, this.functionProperties)) {
			flowBuilder = flowBuilder.channel(outputChannel);
		}
		return flowBuilder;
	}

	/**
	 * Add a {@link Function} bean to the end of an integration flow. The name of the bean
	 * must be provided via `spring.cloud.stream.function.definition` property.
	 * <p>
	 * NOTE: If this method returns true, the integration flow is now represented as a
	 * Reactive Streams {@link Publisher} bean.
	 * </p>
	 * @param flowBuilder instance of the {@link IntegrationFlowBuilder} representing the
	 * current state of the integration flow
	 * @param outputChannel channel where the output of a function will be sent
	 * @param functionProperties the function properties
	 * @return true if {@link Function} was located and added and false if it wasn't.
	 */
	public boolean andThenFunction(IntegrationFlowBuilder flowBuilder,
			MessageChannel outputChannel, StreamFunctionProperties functionProperties) {
		return andThenFunction(flowBuilder.toReactivePublisher(), outputChannel,
				functionProperties);
	}

	/**
	 * @param publisher publisher to subscribe to
	 * @param outputChannel output channel to which a message will be sent
	 * @param functionProperties function properties
	 * @param <I> input of the function
	 * @param <O> output of the function
	 * @return whether the function was properly invoked
	 */
	public <I, O> boolean andThenFunction(Publisher<?> publisher,
			MessageChannel outputChannel, StreamFunctionProperties functionProperties) {
		if (!StringUtils.hasText(functionProperties.getDefinition())) {
			return false;
		}
		FunctionInvoker<I, O> functionInvoker = new FunctionInvoker<>(functionProperties,
				this.functionCatalog, this.functionInspector,
				this.messageConverterFactory, this.errorChannel);

		if (outputChannel != null) {
			subscribeToInput(functionInvoker, publisher, outputChannel::send);
		}
		else {
			subscribeToInput(functionInvoker, publisher, null);
		}
		return true;
	}

	private <T> boolean catalogContains(Class<T> functionType, String name) {
		return this.functionCatalog.lookup(functionType, name) != null;
	}

	private <O> Mono<Void> subscribeToOutput(Consumer<Message<O>> outputProcessor,
			Publisher<Message<O>> outputPublisher) {

		Flux<Message<O>> output = outputProcessor == null ? Flux.from(outputPublisher)
				: Flux.from(outputPublisher).doOnNext(outputProcessor);
		return output.then();
	}

	@SuppressWarnings("unchecked")
	private <I, O> void subscribeToInput(FunctionInvoker<I, O> functionInvoker,
			Publisher<?> publisher, Consumer<Message<O>> outputProcessor) {

		Flux<?> inputPublisher = Flux.from(publisher);
		subscribeToOutput(outputProcessor,
				functionInvoker.apply((Flux<Message<I>>) inputPublisher)).subscribe();
	}

}
