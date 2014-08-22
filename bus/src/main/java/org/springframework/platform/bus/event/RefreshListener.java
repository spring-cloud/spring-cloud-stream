package org.springframework.platform.bus.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.platform.config.client.RefreshEndpoint;

import java.util.Arrays;

/**
 * @author Spencer Gibb
 */
public class RefreshListener implements ApplicationListener<RefreshRemoteApplicationEvent> {
    private static final Logger logger = LoggerFactory.getLogger(RefreshListener.class);

    @Autowired
    RefreshEndpoint endpoint;

    @Override
    public void onApplicationEvent(RefreshRemoteApplicationEvent event) {
        String[] keys = endpoint.refresh();
        logger.info("Received remote refresh request. Keys refreshed {}", Arrays.asList(keys));
    }
}
