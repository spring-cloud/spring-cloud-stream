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

	public AbstractBusEndpoint(ApplicationEventPublisher context, String appId, BusEndpoint busEndpoint) {
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
