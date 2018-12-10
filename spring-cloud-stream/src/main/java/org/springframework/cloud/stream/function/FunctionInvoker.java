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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;

/**
 *
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Tolga Kavukcu
 *
 * @param <I> the payload type of the input Message
 * @param <O> the payload type of the output Message
 *
 * @since 2.1
 */
class FunctionInvoker<I, O> implements Function<Flux<Message<I>>, Flux<Message<O>>> {

	private static final Log logger = LogFactory.getLog(FunctionInvoker.class);

	private final Class<?> inputClass;

	private final Class<?> outputClass;

	private final Function<Flux<?>, Flux<?>> userFunction;

	private final CompositeMessageConverter messageConverter;

	private final MessageChannel errorChannel;

	private final boolean isInputArgumentMessage;

	private final ConsumerProperties consumerProperties;

	private final ProducerProperties producerProperties;

	FunctionInvoker(StreamFunctionProperties functionProperties, FunctionCatalogWrapper functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this(functionProperties, functionCatalog, functionInspector, compositeMessageConverterFactory, null);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	FunctionInvoker(StreamFunctionProperties functionProperties, FunctionCatalogWrapper functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory compositeMessageConverterFactory, MessageChannel errorChannel) {
		Object originalUserFunction = functionCatalog.lookup(functionProperties.getDefinition());

		this.userFunction = originalUserFunction instanceof Consumer
				? new FluxedConsumerWrapper<>((Consumer) originalUserFunction)
						: (Function<Flux<?>, Flux<?>>) originalUserFunction;

		Assert.isInstanceOf(Function.class, this.userFunction);
		this.messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();
		FunctionType functionType = functionInspector.getRegistration(originalUserFunction).getType();
		this.isInputArgumentMessage = functionType.isMessage();
		this.inputClass = functionType.getInputType();
		this.outputClass = functionType.getOutputType();
		this.errorChannel = errorChannel;
		this.consumerProperties = functionProperties.getConsumerProperties() == null
				? new ConsumerProperties() : functionProperties.getConsumerProperties();
		this.producerProperties = functionProperties.getProducerProperties() == null
				? new ProducerProperties() : functionProperties.getProducerProperties();
	}

	@Override
	public Flux<Message<O>> apply(Flux<Message<I>> input) {
		AtomicReference<Message<I>> originalMessageRef = new AtomicReference<>();

		return input.concatMap(message -> {
			return Flux.just(message)
						.doOnNext(originalMessageRef::set)
						.map(this::resolveArgument)
						.transform(this.userFunction::apply)
						.retryBackoff(consumerProperties.getMaxAttempts(),
								Duration.ofMillis(consumerProperties.getBackOffInitialInterval()),
								Duration.ofMillis(consumerProperties.getBackOffMaxInterval()))
						.onErrorResume(e -> {
							onError(e, originalMessageRef.get());
							return Mono.empty();
						});
		}).log().map(resultMessage -> toMessage(resultMessage, originalMessageRef.get())); // create output message
	}

	private void onError(Throwable t, Message<I> originalMessage) {
		if (this.errorChannel != null) {
			ErrorMessage em = new ErrorMessage(t, (Message<?>) originalMessage);
			logger.error(em);
			this.errorChannel.send(em);
		}
		else {
			logger.error(t);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> Message<O> toMessage(T value, Message<I> originalMessage) {
		if (logger.isDebugEnabled()) {
			logger.debug("Converting result back to message using the original message: " + originalMessage);
		}

		Message<O> returnMessage;
		if(producerProperties.isUseNativeEncoding()){
			if (logger.isDebugEnabled()) {
				logger.debug("Native encoding enabled wrapping result to message using the original message: " + originalMessage);
			}
			returnMessage = wrapOutputToMessage(value,originalMessage);
		}
		else {
			returnMessage = (Message<O>)
					(value instanceof Message
							? value
							: this.messageConverter.toMessage(value, originalMessage.getHeaders(), this.outputClass));
			if (returnMessage == null) {
				if (value.getClass().isAssignableFrom(this.outputClass)) {
					returnMessage = wrapOutputToMessage(value, originalMessage);
				}
			}
			Assert.notNull(returnMessage, "Failed to convert result value '" + value + "' to message.");
		}
		return returnMessage;
	}

	@SuppressWarnings("unchecked")
	private <T> Message<O> wrapOutputToMessage(T value, Message<I> originalMessage) {
		Message<O> returnMessage = (Message<O>) MessageBuilder.withPayload(value).copyHeaders(originalMessage.getHeaders())
				.removeHeader(MessageHeaders.CONTENT_TYPE).build();
		return returnMessage;
	}

	@SuppressWarnings("unchecked")
	private <T> T resolveArgument(Message<I> message) {
		if (logger.isDebugEnabled()) {
			logger.debug("Resolving input argument from message: " + message);
		}

		T argument = (T) (shouldConvertFromMessage(message)
				? this.messageConverter.fromMessage(message, this.inputClass)
				: message);
		Assert.notNull(argument, "Failed to resolve argument type '" + this.inputClass + "' from message: " + message );
		if (!this.isInputArgumentMessage && argument instanceof Message) {
			argument = ((Message<T>)argument).getPayload();
		}
		return argument;
	}

	private boolean shouldConvertFromMessage(Message<?> message) {
		return !this.inputClass.isAssignableFrom(Message.class) &&
				!this.inputClass.isAssignableFrom(message.getPayload().getClass()) &&
				!this.inputClass.isAssignableFrom(Object.class);
	}

}
