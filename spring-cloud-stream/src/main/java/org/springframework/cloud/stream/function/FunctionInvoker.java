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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import reactor.core.publisher.Flux;

import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
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

	FunctionInvoker(String functionName, FunctionCatalogWrapper functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this(functionName, functionCatalog, functionInspector, compositeMessageConverterFactory, null);
	}

	FunctionInvoker(String functionName, FunctionCatalogWrapper functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory compositeMessageConverterFactory, MessageChannel errorChannel) {
		this.userFunction = functionCatalog.lookup(functionName);
		Assert.isInstanceOf(Function.class, this.userFunction);
		this.messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();
		FunctionType functionType = functionInspector.getRegistration(this.userFunction).getType();
		this.isInputArgumentMessage = functionType.isMessage();
		this.inputClass = functionType.getInputType();
		this.outputClass = functionType.getOutputType();
		this.errorChannel = errorChannel;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Flux<Message<O>> apply(Flux<Message<I>> input) {
		AtomicReference<Message<I>> originalMessageRef = new AtomicReference<>();
		return input
				.doOnNext(originalMessageRef::set)        // to preserve the original message
				.map(this::resolveArgument)                // resolves argument type before invocation of user function
				.onErrorContinue((x, y) -> onError(x, (Message<I>) y))
				.transform(this.userFunction::apply)    // invoke user function
				.onErrorContinue((x, y) -> onError(x, (Message<I>) y))
				.map(resultMessage -> toMessage(resultMessage, originalMessageRef.get())); // create output message
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
		Message<O> returnMessage = (Message<O>)
				(value instanceof Message
						? value
						: this.messageConverter.toMessage(value, originalMessage.getHeaders(), this.outputClass));
		if (returnMessage == null) {
			if (value.getClass().isAssignableFrom(this.outputClass)) {
				returnMessage = (Message<O>) MessageBuilder.withPayload(value).copyHeaders(originalMessage.getHeaders()).removeHeader(MessageHeaders.CONTENT_TYPE).build();
			}
		}
		Assert.notNull(returnMessage, "Failed to convert result value '" + value + "' to message.");
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
				!message.getPayload().getClass().isAssignableFrom(this.inputClass) &&
				!this.inputClass.isAssignableFrom(Object.class);
	}

}
