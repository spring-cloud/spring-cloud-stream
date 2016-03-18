package org.springframework.cloud.bus.event;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.endpoint.RefreshEndpoint;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 */
public class RefreshListener
		implements ApplicationListener<RefreshRemoteApplicationEvent> {

	private static Log log = LogFactory.getLog(RefreshListener.class);

	private RefreshEndpoint endpoint;

	public RefreshListener(RefreshEndpoint endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
		String[] keys = endpoint.refresh();
		log.info(
				"Received remote refresh request. Keys refreshed " + Arrays.asList(keys));
	}
}
