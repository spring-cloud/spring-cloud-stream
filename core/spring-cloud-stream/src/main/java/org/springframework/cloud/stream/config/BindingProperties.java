/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.validation.annotation.Validated;

/**
 * Contains the properties of a binding.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 * @author Omer Celik
 */
@JsonInclude(Include.NON_DEFAULT)
@Validated
public class BindingProperties {

	/**
	 * Default content type for bindings.
	 */
	public static final MimeType DEFAULT_CONTENT_TYPE = MimeTypeUtils.APPLICATION_JSON;

	private static final String COMMA = ",";

	/**
	 * Function that corresponds to the error handler of the underlying function.
	 */
	private String errorHandlerDefinition;

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

	// Properties for both input and output bindings

	/**
	 * Specifies content-type that will be used by this binding in the event it is not
	 * specified in Message headers. Default: 'application/json'.
	 */
	private String contentType = DEFAULT_CONTENT_TYPE.toString();

	/**
	 * The name of the binder to use for this binding in the event multiple binders
	 * available (e.g., 'rabbit').
	 */
	private String binder;

	/**
	 * Additional consumer specific properties (see {@link ConsumerProperties}).
	 */
	@NestedConfigurationProperty
	private ConsumerProperties consumer;

	/**
	 * Additional producer specific properties (see {@link ProducerProperties}).
	 */
	@NestedConfigurationProperty
	private ProducerProperties producer;

	public String getDestination() {
		return this.destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getGroup() {
		return this.group;
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
		return this.binder;
	}

	public void setBinder(String binder) {
		this.binder = binder;
	}

	public ConsumerProperties getConsumer() {
		return this.consumer;
	}

	public void setConsumer(ConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public ProducerProperties getProducer() {
		return this.producer;
	}

	public void setProducer(ProducerProperties producer) {
		this.producer = producer;
	}

	public boolean onlyOneOfProducerOrConsumerSet() {
		return this.consumer == null || this.producer == null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("destination=").append(this.destination);
		sb.append(COMMA);
		sb.append("group=").append(this.group);
		sb.append(COMMA);
		if (this.contentType != null) {
			sb.append("contentType=").append(this.contentType);
			sb.append(COMMA);
		}
		if (this.binder != null) {
			sb.append("binder=").append(this.binder);
			sb.append(COMMA);
		}
		sb.deleteCharAt(sb.lastIndexOf(COMMA));
		return "BindingProperties{" + sb.toString() + "}";
	}

	public String getErrorHandlerDefinition() {
		return errorHandlerDefinition;
	}

	public void setErrorHandlerDefinition(String errorHandlerDefinition) {
		this.errorHandlerDefinition = errorHandlerDefinition;
	}

}
