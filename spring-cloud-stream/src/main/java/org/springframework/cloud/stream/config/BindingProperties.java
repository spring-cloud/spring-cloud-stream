/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.config;

import javax.validation.constraints.AssertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Contains the properties of a binding.
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@JsonInclude(Include.NON_DEFAULT)
@Validated
public class BindingProperties {

	private static final String COMMA = ",";

	/**
	 * The physical name at the broker that the binder binds to.
	 */
	private String destination;

	/**
	 * Unique name that the binding belongs to (applies to consumers only). Multiple
	 * consumers within the same group share the subscription. A null or empty String
	 * value indicates an anonymous group that is not shared.
	 * @see org.springframework.cloud.stream.binder.Binder#bindConsumer(java.lang.String,
	 * java.lang.String, java.lang.Object,
	 * org.springframework.cloud.stream.binder.ConsumerProperties)
	 */
	private String group;

	// Properties for both inbound/outbound

	private String contentType;

	private String binder;

	private ConsumerProperties consumer;

	private ProducerProperties producer;

	public String getDestination() {
		return this.destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getBinder() {
		return binder;
	}

	public void setBinder(String binder) {
		this.binder = binder;
	}

	public ConsumerProperties getConsumer() {
		return consumer;
	}

	public void setConsumer(ConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public ProducerProperties getProducer() {
		return producer;
	}

	public void setProducer(ProducerProperties producer) {
		this.producer = producer;
	}

	@AssertTrue(message = "A binding must not set both producer and consumer properties.")
	public boolean onlyOneOfProducerOrConsumerSet() {
		return consumer == null || producer == null;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("destination=" + this.destination);
		sb.append(COMMA);
		sb.append("group=" + this.group);
		sb.append(COMMA);
		if (this.contentType != null) {
			sb.append("contentType=" + this.contentType);
			sb.append(COMMA);
		}
		if (this.binder != null) {
			sb.append("binder=" + this.binder);
			sb.append(COMMA);
		}
		sb.deleteCharAt(sb.lastIndexOf(COMMA));
		return "BindingProperties{" + sb.toString() + "}";
	}

}
