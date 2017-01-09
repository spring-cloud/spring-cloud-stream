package org.springframework.cloud.bus.endpoint;

import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Spencer Gibb
 */
@ManagedResource
public class RefreshBusEndpoint extends AbstractBusEndpoint {

	public RefreshBusEndpoint(ApplicationEventPublisher context, String id,
			BusEndpoint delegate) {
		super(context, id, delegate);
	}

	@RequestMapping(value = "refresh", method = RequestMethod.POST)
	@ResponseBody
	@ManagedOperation
	public void refresh(
			@RequestParam(value = "destination", required = false) String destination) {
		publish(new RefreshRemoteApplicationEvent(this, getInstanceId(), destination));
	}

}
