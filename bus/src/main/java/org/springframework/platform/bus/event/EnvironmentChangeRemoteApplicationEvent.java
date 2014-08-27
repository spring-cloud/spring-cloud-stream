package org.springframework.platform.bus.event;

import lombok.Data;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
@Data
public class EnvironmentChangeRemoteApplicationEvent extends RemoteApplicationEvent {
    private final Map<String, String> values;

    public EnvironmentChangeRemoteApplicationEvent(Object source, String originService,
                                                   String destinationService,
                                                   Map<String, String> values) {
        super(source, originService, destinationService);
        this.values = values;
    }
}
