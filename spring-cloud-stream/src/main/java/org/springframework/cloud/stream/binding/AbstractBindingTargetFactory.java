package org.springframework.cloud.stream.binding;

import org.springframework.util.Assert;

/**
 * A {@link BindingTargetFactory} implementation that restricts the type of binding target
 * to a specified class and its supertypes.
 *
 * @author Marius Bogoevici
 */
public abstract class AbstractBindingTargetFactory<T> implements BindingTargetFactory {

	private final Class<T> bindingTargetType;

	protected AbstractBindingTargetFactory(Class<T> bindingTargetType) {
		Assert.notNull(bindingTargetType, "The bound element class cannot be null");
		this.bindingTargetType = bindingTargetType;
	}

	@Override
	public final boolean canCreate(Class<?> clazz) {
		return clazz.isAssignableFrom(this.bindingTargetType);
	}

	@Override
	public abstract T createInput(String name);

	@Override
	public abstract T createOutput(String name);
}
