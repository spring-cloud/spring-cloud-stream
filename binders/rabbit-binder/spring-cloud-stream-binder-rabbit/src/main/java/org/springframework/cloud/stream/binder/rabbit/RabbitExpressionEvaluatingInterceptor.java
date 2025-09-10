/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.util.Assert;

/**
 * Interceptor to evaluate expressions for outbound messages before serialization.
 *
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @since 2.0
 *
 */
public class RabbitExpressionEvaluatingInterceptor implements ChannelInterceptor {

	/**
	 * Instance of ExpressionParser.
	 */
	public static final ExpressionParser PARSER = new SpelExpressionParser();

	/**
	 * Default name for routing key header.
	 */
	public static final String ROUTING_KEY_HEADER = "scst_routingKey";

	/**
	 * Default name for delay header.
	 */
	public static final String DELAY_HEADER = "scst_delay";

	private final Expression routingKeyExpression;

	private final Expression delayExpression;

	private final EvaluationContext evaluationContext;

	/**
	 * Construct an instance with the provided expressions and evaluation context. At
	 * least one expression muse be non-null.
	 * @param routingKeyExpression the routing key expresssion.
	 * @param delayExpression the delay expression.
	 * @param evaluationContext the evaluation context.
	 */
	public RabbitExpressionEvaluatingInterceptor(Expression routingKeyExpression,
			Expression delayExpression, EvaluationContext evaluationContext) {
		Assert.isTrue(routingKeyExpression != null || delayExpression != null,
				"At least one expression is required");
		Assert.notNull(evaluationContext, "the 'evaluationContext' cannot be null");
		if (routingKeyExpression != null) {
			this.routingKeyExpression = routingKeyExpression;
		}
		else {
			this.routingKeyExpression = null;
		}
		if (delayExpression != null) {
			this.delayExpression = delayExpression;
		}
		else {
			this.delayExpression = null;
		}
		this.evaluationContext = evaluationContext;
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {
		MessageBuilder<?> builder = MessageBuilder.fromMessage(message);
		if (this.routingKeyExpression != null) {
			builder.setHeader(ROUTING_KEY_HEADER,
					this.routingKeyExpression.getValue(this.evaluationContext, message));
		}
		if (this.delayExpression != null) {
			builder.setHeader(DELAY_HEADER,
					this.delayExpression.getValue(this.evaluationContext, message));
		}
		return builder.build();
	}

}
