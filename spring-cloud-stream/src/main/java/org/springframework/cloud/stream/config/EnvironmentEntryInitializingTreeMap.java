package org.springframework.cloud.stream.config;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.boot.bind.PropertySourcesPropertyValues;
import org.springframework.boot.bind.RelaxedDataBinder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.Assert;

/**
 * A {@link Map} implementation that initializes its entries by binding values from the
 * supplied application context's environment. Any call to 'get()' will result in either
 * returning the existing value or initializing a new entry by binding properties with the
 * specified prefix from the environment.
 *
 * This is strictly intended to be used for configuration property values and not to be
 * used as a general purpose map.
 *
 * This implementation is not thread safe.
 *
 * @author Marius Bogoevici
 */
public class EnvironmentEntryInitializingTreeMap<T> extends AbstractMap<String, T> {

	private final ConfigurableApplicationContext applicationContext;

	private final Class<T> entryClass;

	private final String defaultsPrefix;

	private final Map<String, T> delegate;

	/**
	 * Constructs the map.
	 * 
	 * @param applicationContext the application context that supplies the property values
	 * @param entryClass the entry class
	 * @param defaultsPrefix the prefix for initializing the properties
	 * @param delegate the actual map that stores the values
	 */
	public EnvironmentEntryInitializingTreeMap(ConfigurableApplicationContext applicationContext, Class<T> entryClass,
			String defaultsPrefix, Map<String, T> delegate) {
		Assert.notNull(applicationContext, "The context cannot be null");
		Assert.notNull(entryClass, "The entry class cannot be null");
		Assert.notNull(defaultsPrefix, "The prefix for the property defaults cannot be null");
		Assert.notNull(delegate, "The delegate cannot be null");
		this.applicationContext = applicationContext;
		this.entryClass = entryClass;
		this.defaultsPrefix = defaultsPrefix;
		this.delegate = delegate;
	}

	@Override
	public T get(Object key) {
		if (!this.delegate.containsKey(key) && key instanceof String) {
			T entry = BeanUtils.instantiate(entryClass);
			if (applicationContext != null) {
				RelaxedDataBinder defaultsDataBinder = new RelaxedDataBinder(entry, defaultsPrefix);
				defaultsDataBinder.bind(
						new PropertySourcesPropertyValues(applicationContext.getEnvironment().getPropertySources()));
			}
			this.delegate.put((String) key, entry);
		}
		return this.delegate.get(key);
	}

	@Override
	public Set<Entry<String, T>> entrySet() {
		return delegate.entrySet();
	}

}
