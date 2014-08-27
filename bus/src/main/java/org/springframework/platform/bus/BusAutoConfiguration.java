package org.springframework.platform.bus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.platform.bus.endpoint.BusEndpoint;
import org.springframework.platform.bus.endpoint.EnvironmentBusEndpoint;
import org.springframework.platform.bus.endpoint.RefreshBusEndpoint;
import org.springframework.platform.bus.event.EnvironmentChangeListener;
import org.springframework.platform.bus.event.RefreshListener;
import org.springframework.platform.config.client.RefreshEndpoint;
import org.springframework.platform.context.environment.EnvironmentManager;

/**
 * @author Spencer Gibb
 */
@Configuration
@ConditionalOnExpression("${bus.enabled:true}")
public class BusAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(BusAutoConfiguration.class);

    @Bean
    public BusEndpoint busEndpoint() {
        return new BusEndpoint();
    }

    @ConditionalOnClass(RefreshEndpoint.class)
    @ConditionalOnBean(RefreshEndpoint.class)
    protected static class BusRefreshConfiguration {
        @Bean
        @ConditionalOnExpression("${bus.refresh.enabled:true}")
        public RefreshListener refreshListener() {
            return new RefreshListener();
        }

        @Bean
        @ConditionalOnExpression("${endpoints.bus.refresh.enabled:true}")
        public RefreshBusEndpoint refreshBusEndpoint() {
            return new RefreshBusEndpoint();
        }
    }

    @ConditionalOnClass(EnvironmentManager.class)
    @ConditionalOnBean(EnvironmentManager.class)
    protected static class BusEnvironmentConfiguration {
        @Bean
        @ConditionalOnExpression("${bus.env.enabled:true}")
        public EnvironmentChangeListener environmentChangeListener() {
            return new EnvironmentChangeListener();
        }

        @Bean
        @ConditionalOnExpression("${endpoints.bus.env.enabled:true}")
        public EnvironmentBusEndpoint environmentBusEndpoint() {
            return new EnvironmentBusEndpoint();
        }
    }
}
