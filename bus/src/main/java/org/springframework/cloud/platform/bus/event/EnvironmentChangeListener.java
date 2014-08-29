package org.springframework.cloud.bus.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.cloud.context.environment.EnvironmentManager;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
public class EnvironmentChangeListener implements ApplicationListener<EnvironmentChangeRemoteApplicationEvent> {
    private static final Logger logger = LoggerFactory.getLogger(EnvironmentChangeListener.class);

    @Autowired
    private EnvironmentManager env;

    @Override
    public void onApplicationEvent(EnvironmentChangeRemoteApplicationEvent event) {
        Map<String, String> values = event.getValues();
        logger.info("Received remote environment change request. Keys/values to update {}", values);
        for (Map.Entry<String, String> entry: values.entrySet()) {
            env.setProperty(entry.getKey(), entry.getValue());
        }
    }
}
