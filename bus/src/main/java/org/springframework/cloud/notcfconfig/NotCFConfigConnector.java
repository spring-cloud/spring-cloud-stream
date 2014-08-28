package org.springframework.cloud.notcfconfig;

import org.springframework.cloud.localconfig.LocalConfigConnector;
import org.springframework.cloud.service.UriBasedServiceData;
import org.springframework.cloud.util.EnvironmentAccessor;

import java.util.Collections;
import java.util.List;

/**
 * @author Spencer Gibb
 * Let's spring boots current auto-configuration work, effectivly disabling spring cloud local
 * TODO: workaround for not having to configure the local cloud connector
 */
public class NotCFConfigConnector extends LocalConfigConnector {

    private EnvironmentAccessor env = new EnvironmentAccessor();

    @Override
    public boolean isInMatchingCloud() {
        return env.getEnvValue("VCAP_APPLICATION") == null;
    }

    @Override
    protected List<UriBasedServiceData> getServicesData() {
        return Collections.emptyList();
    }
}
