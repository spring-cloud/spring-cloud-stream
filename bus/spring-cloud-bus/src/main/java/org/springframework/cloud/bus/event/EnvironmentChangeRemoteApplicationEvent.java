package org.springframework.cloud.bus.event;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
public class EnvironmentChangeRemoteApplicationEvent extends RemoteApplicationEvent {

	private final Map<String, String> values;

	@SuppressWarnings("unused")
	private EnvironmentChangeRemoteApplicationEvent() {
		// for serializers
		values = null;
	}

	public EnvironmentChangeRemoteApplicationEvent(Object source, String originService,
			String destinationService, Map<String, String> values) {
		super(source, originService, destinationService);
		this.values = values;
	}

	public Map<String, String> getValues() {
		return values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EnvironmentChangeRemoteApplicationEvent other = (EnvironmentChangeRemoteApplicationEvent) obj;
		if (values == null) {
			if (other.values != null)
				return false;
		}
		else if (!values.equals(other.values))
			return false;
		return true;
	}

}
