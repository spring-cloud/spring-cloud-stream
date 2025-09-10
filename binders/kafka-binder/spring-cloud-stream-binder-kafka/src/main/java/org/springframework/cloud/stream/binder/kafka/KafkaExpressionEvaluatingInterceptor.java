/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.util.Assert;

/**
 * Interceptor to evaluate expressions for outbound messages before serialization.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class KafkaExpressionEvaluatingInterceptor implements ChannelInterceptor {

	/**
	 * Name for the evaluated message key header.
	 */
	public static final String MESSAGE_KEY_HEADER = "scst_messageKey";

	private final Expression messageKeyExpression;

	private final EvaluationContext evaluationContext;

	/**
	 * Construct an instance with the provided expressions and evaluation context. At
	 * least one expression muse be non-null.
	 * @param messageKeyExpression the routing key expression.
	 * @param evaluationContext the evaluation context.
	 */
	public KafkaExpressionEvaluatingInterceptor(Expression messageKeyExpression, EvaluationContext evaluationContext) {
		Assert.notNull(messageKeyExpression != null, "A message key expression is required");
		Assert.notNull(evaluationContext, "the 'evaluationContext' cannot be null");
		this.messageKeyExpression = messageKeyExpression;
		this.evaluationContext = evaluationContext;
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {
		MessageBuilder<?> builder = MessageBuilder.fromMessage(message);
		if (this.messageKeyExpression != null) {
			builder.setHeader(MESSAGE_KEY_HEADER,
					this.messageKeyExpression.getValue(this.evaluationContext, message));
		}
		return builder.build();
	}

}
