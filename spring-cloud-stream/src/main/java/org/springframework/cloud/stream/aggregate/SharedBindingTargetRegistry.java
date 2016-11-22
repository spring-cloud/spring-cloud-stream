package org.springframework.cloud.stream.aggregate;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Stores binding targets shared by the components of an aggregate application.
 * 
 * @author Marius Bogoevici
 * @since 1.1.1
 */
public class SharedBindingTargetRegistry {

	private Map<String, Object> sharedBindingTargets = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

	public <T> T get(String id, Class<T> bindingTargetType) {
		Object sharedBindingTarget = this.sharedBindingTargets.get(id);
		if (sharedBindingTarget == null) {
			return null;
		}
		if (!bindingTargetType.isAssignableFrom(sharedBindingTarget.getClass())) {
			throw new IllegalArgumentException("A shared " + bindingTargetType.getName() + " was requested, "
					+ "but the existing shared target with id '" + id + "' is a " + sharedBindingTarget.getClass());
		}
		else {
			return (T) sharedBindingTarget;
		}
	}

	public void register(String id, Object bindingTarget) {
		this.sharedBindingTargets.put(id, bindingTarget);
	}

	public Map<String, Object> getAll() {
		return Collections.unmodifiableMap(this.sharedBindingTargets);
	}

}
