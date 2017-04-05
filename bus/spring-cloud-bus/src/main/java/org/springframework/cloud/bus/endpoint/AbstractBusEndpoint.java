/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus.endpoint;

import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoint;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

/**
 * @author Spencer Gibb
 */
public class AbstractBusEndpoint implements MvcEndpoint {

	private ApplicationEventPublisher context;

	private BusEndpoint delegate;

	private String appId;

	public AbstractBusEndpoint(ApplicationEventPublisher context, String appId,
			BusEndpoint busEndpoint) {
		this.context = context;
		this.appId = appId;
		this.delegate = busEndpoint;
	}

	protected String getInstanceId() {
		return this.appId;
	}

	protected void publish(ApplicationEvent event) {
		context.publishEvent(event);
	}

	@Override
	public String getPath() {
		return "/" + this.delegate.getId();
	}

	@Override
	public boolean isSensitive() {
		return this.delegate.isSensitive();
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Class<? extends Endpoint> getEndpointType() {
		return this.delegate.getClass();
	}
}
