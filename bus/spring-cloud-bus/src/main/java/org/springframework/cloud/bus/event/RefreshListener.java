package org.springframework.cloud.bus.event;

import java.util.Arrays;

import lombok.extern.apachecommons.CommonsLog;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.config.client.RefreshEndpoint;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 */
@CommonsLog
public class RefreshListener implements
		ApplicationListener<RefreshRemoteApplicationEvent> {

	@Autowired
	private RefreshEndpoint endpoint;

	@Override
	public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
		String[] keys = endpoint.refresh();
		log.info("Received remote refresh request. Keys refreshed " + Arrays.asList(keys));
	}
}
