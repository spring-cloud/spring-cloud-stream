package org.springframework.cloud.bus.event;

import java.util.Arrays;

import lombok.extern.apachecommons.CommonsLog;

import org.springframework.cloud.configure.client.RefreshEndpoint;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 */
@CommonsLog
public class RefreshListener implements
		ApplicationListener<RefreshRemoteApplicationEvent> {
	
	private RefreshEndpoint endpoint;

	public RefreshListener(RefreshEndpoint endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
		String[] keys = endpoint.refresh();
		log.info("Received remote refresh request. Keys refreshed " + Arrays.asList(keys));
	}
}
