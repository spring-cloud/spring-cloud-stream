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

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.util.Assert;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <I> the payload type of the input Message
 * @param <O> the payload type of the output Message
 *
 * @since 2.1
 */
class FunctionInvoker<I, O> implements Function<Flux<Message<I>>, Flux<Message<O>>>  {

	private static final Log logger = LogFactory.getLog(FunctionInvoker.class);

	private final Class<?> inputClass;

	private final Function<Flux<?>, Flux<?>> userFunction;

	private final CompositeMessageConverter messageConverter;

	FunctionInvoker(String functionName, FunctionCatalog functionCatalog, FunctionInspector functionInspector,
						CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this.userFunction = functionCatalog.lookup(Function.class, functionName);
		Assert.notNull(this.userFunction, "userFunction: " + functionName + " can not be located.");
		this.messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();
		FunctionType functionType = functionInspector.getRegistration(this.userFunction).getType();
		this.inputClass = functionType.getInputType();
	}

	@Override
	public Flux<Message<O>> apply(Flux<Message<I>> input) {
		AtomicReference<Message<I>> originalMessage = new AtomicReference<Message<I>>();
		return input
			.doOnNext(originalMessage::set) 		// to preserve the original message
			.map(this::resolveArgument)				// resolves argument type before invocation of user function
			.transform(f -> userFunction.apply(f))	// invoke user function
			.map(resultMessage -> toMessage(resultMessage, originalMessage.get())); // create output message
	}

	@SuppressWarnings("unchecked")
	private <T> Message<O> toMessage(T value, Message<I> originalMessage) {
		if (logger.isDebugEnabled()) {
			logger.debug("Converting result back to message using the original message: " + originalMessage);
		}
		Message<O> result = value instanceof Message
				? result = (Message<O>) value
				: (Message<O>) this.messageConverter.toMessage(value, originalMessage.getHeaders());
		return result;
	}

	@SuppressWarnings("unchecked")
	private <T> T resolveArgument(Message<I> message) {
		if (logger.isDebugEnabled()) {
			logger.debug("Resolving input argument from message: " + message);
		}

		T result = this.shouldConvertFromMessage(message)
				? (T) this.messageConverter.fromMessage(message, this.inputClass)
						: (T) message;
		return result;
	}

	private boolean shouldConvertFromMessage(Message<?> message) {
		return !this.inputClass.isAssignableFrom(byte[].class) &&
				!this.inputClass.isAssignableFrom(Object.class);
	}
}
