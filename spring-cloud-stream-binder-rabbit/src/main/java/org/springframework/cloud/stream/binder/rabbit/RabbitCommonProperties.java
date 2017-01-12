/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import org.springframework.amqp.core.ExchangeTypes;

/**
 * @author Gary Russell
 * @since 1.2
 *
 */
public abstract class RabbitCommonProperties {

	/**
	 * type of exchange to declare (if necessary, and declareExchange is true).
	 */
	private String exchangeType = ExchangeTypes.TOPIC;

	/**
	 * whether to declare the exchange
	 */
	private boolean declareExchange = true;

	/**
	 * whether a delayed message exchange should be used
	 */
	private boolean delayedExchange = false;

	/**
	 * whether to bind a queue (or queues when partitioned) to the exchange
	 */
	private boolean bindQueue = true;

	/**
	 * The routing key to bind (default # for non-partitioned, destination-instanceIndex for partitioned)
	 */
	private String bindingRoutingKey;

	public String getExchangeType() {
		return this.exchangeType;
	}

	public void setExchangeType(String exchangeType) {
		this.exchangeType = exchangeType;
	}

	public boolean isDeclareExchange() {
		return this.declareExchange;
	}

	public void setDeclareExchange(boolean declareExchange) {
		this.declareExchange = declareExchange;
	}

	public boolean isDelayedExchange() {
		return this.delayedExchange;
	}

	public void setDelayedExchange(boolean delayedExchange) {
		this.delayedExchange = delayedExchange;
	}

	public boolean isBindQueue() {
		return this.bindQueue;
	}

	public void setBindQueue(boolean bindQueue) {
		this.bindQueue = bindQueue;
	}

	public String getBindingRoutingKey() {
		return this.bindingRoutingKey;
	}

	public void setBindingRoutingKey(String routingKey) {
		this.bindingRoutingKey = routingKey;
	}

}
