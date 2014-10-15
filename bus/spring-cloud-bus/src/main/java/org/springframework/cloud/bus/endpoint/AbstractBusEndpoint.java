package org.springframework.cloud.bus.endpoint;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoint;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * @author Spencer Gibb
 */
public class AbstractBusEndpoint implements MvcEndpoint {
    @Autowired
    protected ConfigurableEnvironment env;
    @Autowired
    protected ConfigurableApplicationContext context;
    @Autowired
    private BusEndpoint delegate;

    protected String getAppName() {
        return env.getProperty("spring.application.name");
    }

    protected void publish(ApplicationEvent event) {
        context.publishEvent(event);
    }

    @Override
    public String getPath() {
        return "/" + this.delegate.getId();
    }

    @Override
    public boolean isSensitive() {
        return this.delegate.isSensitive();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends Endpoint> getEndpointType() {
        return this.delegate.getClass();
    }
}
