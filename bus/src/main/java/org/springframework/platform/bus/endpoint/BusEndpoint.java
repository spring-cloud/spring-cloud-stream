package org.springframework.platform.bus.endpoint;

import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Spencer Gibb
 */
@ConfigurationProperties(prefix = "endpoints.bus", ignoreUnknownFields = false)
public class BusEndpoint extends AbstractEndpoint<Collection<String>>/* implements MvcEndpoint*/ {

    public BusEndpoint() {
        super("bus");
    }

    @Override
    public Collection<String> invoke() {
        return Collections.EMPTY_LIST;
    }
}
