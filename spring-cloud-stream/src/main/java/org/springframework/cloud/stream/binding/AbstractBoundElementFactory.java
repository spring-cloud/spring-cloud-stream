package org.springframework.cloud.stream.binding;

import org.springframework.util.Assert;

/**
 * A {@link BoundElementFactory} implementation that restricts the type of bound element
 * to a specified class and its supertypes.
 *
 * @author Marius Bogoevici
 */
public abstract class AbstractBoundElementFactory<T> implements BoundElementFactory {

	private final Class<T> boundElementClass;

	protected AbstractBoundElementFactory(Class<T> boundElementClass) {
		Assert.notNull(boundElementClass, "The bound element class cannot be null");
		this.boundElementClass = boundElementClass;
	}

	@Override
	public final boolean canCreate(Class<?> clazz) {
		return clazz.isAssignableFrom(boundElementClass);
	}

	@Override
	public abstract T createInput(String name);

	@Override
	public abstract T createOutput(String name);
}
