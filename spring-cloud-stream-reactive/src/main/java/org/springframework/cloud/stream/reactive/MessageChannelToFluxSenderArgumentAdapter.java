/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoProcessor;

import org.springframework.cloud.stream.binding.StreamListenerArgumentAdapter;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * Adapts an {@link org.springframework.cloud.stream.annotation.Output} annotated
 * {@link FluxSender} to an outbound {@link MessageChannel}.
 * @author Marius Bogoevici
 */
public class MessageChannelToFluxSenderArgumentAdapter
		implements StreamListenerArgumentAdapter<FluxSender, MessageChannel> {

	private Log log = LogFactory.getLog(FluxToMessageChannelResultAdapter.class);

	@Override
	public boolean supports(Class<?> boundElementType, MethodParameter methodParameter) {
		ResolvableType type = ResolvableType.forMethodParameter(methodParameter);
		return MessageChannel.class.isAssignableFrom(boundElementType) && FluxSender.class.isAssignableFrom(
				type.getRawClass());
	}

	@Override
	public FluxSender adapt(MessageChannel boundElement, MethodParameter parameter) {
		return resultPublisher -> {
			MonoProcessor<Void> sendError = MonoProcessor.create();
			Flux<?> outboundFlux;
			if (resultPublisher instanceof Flux) {
				outboundFlux = ((Flux<?>) resultPublisher);
			}
			else {
				outboundFlux = (Flux.merge(resultPublisher));
			}
			outboundFlux
					.doOnError(e -> this.log.error("Error during processing: ", e))
					.retry()
					.subscribe(
							result -> boundElement.send(result instanceof Message<?> ? (Message<?>) result :
									MessageBuilder.withPayload(result).build()), e -> sendError.onError(e),
							() -> sendError.onComplete());
			return sendError;
		};
	}
}
