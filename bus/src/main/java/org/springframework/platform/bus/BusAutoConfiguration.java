package org.springframework.platform.bus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.platform.bus.event.RefreshListener;
import org.springframework.platform.config.client.RefreshEndpoint;

/**
 * @author Spencer Gibb
 */
@Configuration
public class BusAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(BusAutoConfiguration.class);

    @ConditionalOnClass(RefreshEndpoint.class)
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

        @Bean
        public BusEndpoint busEndpoint() {
            return new BusEndpoint();
        }
    }
}
