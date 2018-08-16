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

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.FluxSupplier;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 */
public class IntegrationFlowFunctionSupport {

	private final FunctionCatalog functionCatalog;

	private final FunctionInspector functionInspector;

	private final CompositeMessageConverterFactory messageConverterFactory;

	private final FunctionProperties functionProperties;

	/**
	 * @param functionCatalog
	 * @param functionInspector
	 * @param messageConverterFactory
	 * @param functionProperties
	 */
	public IntegrationFlowFunctionSupport(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory messageConverterFactory, FunctionProperties functionProperties) {

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
	 * Create an instance of the {@link IntegrationFlowBuilder} from a {@link Supplier} bean available in the context.
	 * The name of the bean must be provided via `spring.cloud.stream.function.name` property.
	 * @return instance of {@link IntegrationFlowBuilder}
	 * @throws IllegalStateException if the named Supplier can not be located.
	 */
	public IntegrationFlowBuilder integrationFlowFromNamedSupplier() {
		if (StringUtils.hasText(this.functionProperties.getName())) {
			Supplier<?> supplier = functionCatalog.lookup(Supplier.class, this.functionProperties.getName());
			if (supplier instanceof FluxSupplier) {
				supplier = ((FluxSupplier<?>)supplier).getTarget();
			}
			return integrationFlowFromProvidedSupplier(supplier)
					.split();
		}

		throw new IllegalStateException(
				"A Supplier is not specified in the 'spring.cloud.stream.function.name' property.");
	}

	/**
	 * Create an instance of the {@link IntegrationFlowBuilder} from a provided {@link Supplier}.
	 * @return instance of {@link IntegrationFlowBuilder}
	 */
	public IntegrationFlowBuilder integrationFlowFromProvidedSupplier(Supplier<?> supplier) {
		return IntegrationFlows.from(supplier);
	}

	/**
	 * Add a {@link Function} bean to the end of an integration flow.
	 * The name of the bean must be provided via `spring.cloud.stream.function.name` property.
	 * <p>
	 * NOTE: If this method returns true, the integration flow is now represented
	 * as a Reactive Streams {@link Publisher} bean.
	 * </p>
	 * @param flowBuilder instance of the {@link IntegrationFlowBuilder} representing
	 *                       the current state of the integration flow
	 * @param outputChannel channel where the output of a function will be sent
	 * @return true if {@link Function} was located and added and false if it wasn't.
	 */
	public <I,O> boolean andThenFunction(IntegrationFlowBuilder flowBuilder, MessageChannel outputChannel) {
		if (StringUtils.hasText(this.functionProperties.getName())) {
			FunctionInvoker<I,O> functionInvoker =
					new FunctionInvoker<>(this.functionProperties.getName(), this.functionCatalog,
							this.functionInspector, this.messageConverterFactory);

			subscribeToInput(functionInvoker, flowBuilder.toReactivePublisher(), outputChannel::send);
			return true;
		}
		return false;
	}

	private <O> Mono<Void> subscribeToOutput(Consumer<Message<O>> outputProcessor,
			Publisher<Message<O>> outputPublisher) {

		Flux<Message<O>> output = outputProcessor == null
				? Flux.from(outputPublisher)
						: Flux.from(outputPublisher).doOnNext(outputProcessor);
		return output.then();
	}

	private <I,O> void subscribeToInput(FunctionInvoker<I,O> functionInvoker, Publisher<Message<I>> publisher,
			Consumer<Message<O>> outputProcessor) {

		Flux<Message<I>> inputPublisher = Flux.from(publisher);
		subscribeToOutput(outputProcessor, functionInvoker.apply(inputPublisher)).subscribe();
	}

}
