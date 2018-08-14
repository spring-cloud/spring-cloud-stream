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

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;



/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 */
public class FunctionSupport {

	private final FunctionCatalog functionCatalog;

	private final FunctionInspector functionInspector;

	private final CompositeMessageConverterFactory messageConverterFactory;

	public FunctionSupport(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory messageConverterFactory) {

		Assert.notNull(functionCatalog, "'functionCatalog' must not be null");
		Assert.notNull(functionInspector, "'functionInspector' must not be null");
		Assert.notNull(messageConverterFactory, "'messageConverterFactory' must not be null");
		this.functionCatalog = functionCatalog;
		this.functionInspector = functionInspector;
		this.messageConverterFactory = messageConverterFactory;
	}

	public <I,O> void applyFunctionToIntegrationFlow(String functionName, IntegrationFlowBuilder flowBuilder,
			Consumer<Message<O>> outputProcessor) {

		FunctionInvoker<I,O> functionInvoker =
				new FunctionInvoker<>(functionName, this.functionCatalog, this.functionInspector,
						this.messageConverterFactory);

		subscribeToInput(functionInvoker, flowBuilder.toReactivePublisher(), outputProcessor);
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
