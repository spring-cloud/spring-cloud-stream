package org.springframework.cloud.bus.event;

import java.util.Map;

import lombok.extern.apachecommons.CommonsLog;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.environment.EnvironmentManager;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 */
@CommonsLog
public class EnvironmentChangeListener implements
		ApplicationListener<EnvironmentChangeRemoteApplicationEvent> {
	@Autowired
	private EnvironmentManager env;

	@Override
	public void onApplicationEvent(EnvironmentChangeRemoteApplicationEvent event) {
		Map<String, String> values = event.getValues();
		log.info("Received remote environment change request. Keys/values to update "
				+ values);
		for (Map.Entry<String, String> entry : values.entrySet()) {
			env.setProperty(entry.getKey(), entry.getValue());
		}
	}
}
