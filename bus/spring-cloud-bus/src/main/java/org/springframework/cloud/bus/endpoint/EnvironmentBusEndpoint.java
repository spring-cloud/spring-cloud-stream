package org.springframework.cloud.bus.endpoint;

import java.util.Map;

import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Spencer Gibb
 */
@ManagedResource
public class EnvironmentBusEndpoint extends AbstractBusEndpoint {

	public EnvironmentBusEndpoint(ApplicationEventPublisher context, String id,
			BusEndpoint delegate) {
		super(context, id, delegate);
	}

	@RequestMapping(value = "env", method = RequestMethod.POST)
	@ResponseBody
	@ManagedOperation
	// TODO: make this an abstract method in AbstractBusEndpoint?
	public void env(@RequestParam Map<String, String> params,
			@RequestParam(value = "destination", required = false) String destination) {
		publish(new EnvironmentChangeRemoteApplicationEvent(this, getInstanceId(),
				destination, params));
	}

}
