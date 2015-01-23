package org.springframework.cloud.bus.jackson;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnBusEnabled
@ConditionalOnClass({ RefreshBusEndpoint.class, ObjectMapper.class })
@AutoConfigureAfter(BusAutoConfiguration.class)
public class BusJacksonAutoConfiguration {

	@Bean
	public SubtypeModule basicBusSubtypeModule() {
		return new SubtypeModule(RefreshRemoteApplicationEvent.class,
				EnvironmentChangeRemoteApplicationEvent.class);
	}
}
