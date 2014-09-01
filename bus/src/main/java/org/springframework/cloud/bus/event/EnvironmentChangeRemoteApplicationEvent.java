package org.springframework.cloud.bus.event;

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
@Data
@EqualsAndHashCode(callSuper=false)
public class EnvironmentChangeRemoteApplicationEvent extends RemoteApplicationEvent {

	private final Map<String, String> values;

    public EnvironmentChangeRemoteApplicationEvent(Object source, String originService,
                                                   String destinationService,
                                                   Map<String, String> values) {
        super(source, originService, destinationService);
        this.values = values;
    }

}
