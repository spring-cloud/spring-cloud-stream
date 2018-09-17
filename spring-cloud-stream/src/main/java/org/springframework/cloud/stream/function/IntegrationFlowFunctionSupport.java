/*
 * Copyright 2018 the original author or authors.
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
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 *
 * @since 2.1
 */
public class IntegrationFlowFunctionSupport {

	private final FunctionCatalogWrapper functionCatalog;

	private final FunctionInspector functionInspector;

	private final CompositeMessageConverterFactory messageConverterFactory;

	private final StreamFunctionProperties functionProperties;

	@Autowired
	private MessageChannel errorChannel;

	@Autowired(required = false)
	private Source source;

	@Autowired(required = false)
	private Processor processor;

	@Autowired(required = false)
	private Sink sink;

	/**
	 * @param functionCatalog
	 * @param functionInspector
	 * @param messageConverterFactory
	 * @param functionProperties
	 */
	IntegrationFlowFunctionSupport(FunctionCatalogWrapper functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory messageConverterFactory, StreamFunctionProperties functionProperties) {

		Assert.notNull(functionCatalog, "'functionCatalog' must not be null");
		Assert.notNull(functionInspector, "'functionInspector' must not be null");
		Assert.notNull(messageConverterFactory, "'messageConverterFactory' must not be null");
		Assert.notNull(functionProperties, "'functionProperties' must not be null");
		this.functionCatalog = functionCatalog;
		this.functionInspector = functionInspector;
		this.messageConverterFactory = messageConverterFactory;
		this.functionProperties = functionProperties;
	}

	/**
	 * Determines if function specified via 'spring.cloud.stream.function.definition'
	 * property can be located in {@link FunctionCatalog}
	 *
	 * @param typeOfFunction must be Supplier, Function or Consumer
	 * @return
	 */
	public <T> boolean containsFunction(Class<T> typeOfFunction) {
		return StringUtils.hasText(this.functionProperties.getDefinition())
				&& this.functionCatalog.contains(typeOfFunction, this.functionProperties.getDefinition());
	}

	/**
	 * Determines if function specified via 'spring.cloud.stream.function.definition'
	 * property can be located in {@link FunctionCatalog}
	 *
	 * @param typeOfFunction must be Supplier, Function or Consumer
	 * @param functionName the function name to check
	 * @return
	 */
	public <T> boolean containsFunction(Class<T> typeOfFunction, String functionName) {
		return StringUtils.hasText(functionName)
				&& this.functionCatalog.contains(typeOfFunction, functionName);
	}

	public FunctionType getCurrentFunctionType() {
		FunctionType functionType = functionInspector.getRegistration(
				functionCatalog.lookup(this.functionProperties.getDefinition())).getType();
		return functionType;
	}

	/**
	 * Create an instance of the {@link IntegrationFlowBuilder} from a {@link Supplier} bean available in the context.
	 * The name of the bean must be provided via `spring.cloud.stream.function.definition` property.
	 * @return instance of {@link IntegrationFlowBuilder}
	 * @throws IllegalStateException if the named Supplier can not be located.
	 */
	public IntegrationFlowBuilder integrationFlowFromNamedSupplier() {
		if (StringUtils.hasText(this.functionProperties.getDefinition())) {
			Supplier<?> supplier = functionCatalog.lookup(Supplier.class, this.functionProperties.getDefinition());
			if (supplier instanceof FluxSupplier) {
				supplier = ((FluxSupplier<?>)supplier).getTarget();
			}
			return integrationFlowFromProvidedSupplier(supplier)
					.split();
		}

		throw new IllegalStateException(
				"A Supplier is not specified in the 'spring.cloud.stream.function.definition' property.");
	}

	/**
	 * Create an instance of the {@link IntegrationFlowBuilder} from a provided {@link Supplier}.
	 * @return instance of {@link IntegrationFlowBuilder}
	 */
	public IntegrationFlowBuilder integrationFlowFromProvidedSupplier(Supplier<?> supplier) {
		return IntegrationFlows.from(supplier);
	}

	/**
	 * @param inputChannel
	 * @return
	 */
	public <O> IntegrationFlowBuilder integrationFlowFromChannel(SubscribableChannel inputChannel) {
		IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(inputChannel).bridge();
		return flowBuilder;
	}

	public <O> IntegrationFlowBuilder integrationFlowForFunction(SubscribableChannel inputChannel,
			MessageChannel outputChannel) {
		IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(inputChannel).bridge();

		if (!this.andThenFunction(flowBuilder, outputChannel, this.functionProperties.getDefinition())) {
			flowBuilder = flowBuilder.channel(outputChannel);
		}
		return flowBuilder;
	}

	/**
	 * Add a {@link Function} bean to the end of an integration flow.
	 * The name of the bean must be provided via `spring.cloud.stream.function.definition` property.
	 * <p>
	 * NOTE: If this method returns true, the integration flow is now represented
	 * as a Reactive Streams {@link Publisher} bean.
	 * </p>
	 * @param flowBuilder instance of the {@link IntegrationFlowBuilder} representing
	 *                       the current state of the integration flow
	 * @param outputChannel channel where the output of a function will be sent
	 * @param functionName the function name to use
	 * @return true if {@link Function} was located and added and false if it wasn't.
	 */
	public <I,O> boolean andThenFunction(IntegrationFlowBuilder flowBuilder, MessageChannel outputChannel,
			String functionName) {
		return andThenFunction(flowBuilder.toReactivePublisher(), outputChannel, functionName);
	}

	public <I,O> boolean andThenFunction(Publisher<?> publisher, MessageChannel outputChannel,
			String functionName) {
		if (!StringUtils.hasText(functionName)) {
			return false;
		}
		FunctionInvoker<I, O> functionInvoker =
				new FunctionInvoker<>(functionName, this.functionCatalog,
						this.functionInspector, this.messageConverterFactory, this.errorChannel);

		if (outputChannel != null) {
			subscribeToInput(functionInvoker, publisher, outputChannel::send);
		}
		else {
			subscribeToInput(functionInvoker, publisher, null);
		}
		return true;
	}

	private <O> Mono<Void> subscribeToOutput(Consumer<Message<O>> outputProcessor,
			Publisher<Message<O>> outputPublisher) {

		Flux<Message<O>> output = outputProcessor == null
				? Flux.from(outputPublisher)
						: Flux.from(outputPublisher).doOnNext(outputProcessor);
		return output.then();
	}

	@SuppressWarnings("unchecked")
	private <I,O> void subscribeToInput(FunctionInvoker<I,O> functionInvoker, Publisher<?> publisher,
			Consumer<Message<O>> outputProcessor) {

		Flux<?> inputPublisher = Flux.from(publisher);
		subscribeToOutput(outputProcessor, functionInvoker.apply((Flux<Message<I>>) inputPublisher)).subscribe();
	}
}
