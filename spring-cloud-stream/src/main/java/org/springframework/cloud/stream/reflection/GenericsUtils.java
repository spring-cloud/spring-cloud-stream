package org.springframework.cloud.stream.reflection;

import org.springframework.core.ResolvableType;

/**
 * Internal utilities for handling generics.
 *
 * @author Marius Bogoevici
 */
public class GenericsUtils {

	public static Class<?> getParameter(Class<?> evaluatedClass, Class<?> interfaceClazz, int index) {
		Class<?> bindableType = null;
		ResolvableType currentType = ResolvableType.forType(evaluatedClass);
		while (!Object.class.equals(currentType.getRawClass()) && bindableType == null) {
			ResolvableType[] interfaces = currentType.getInterfaces();
			ResolvableType resolvableType = null;
			for (ResolvableType interfaceType : interfaces) {
				if (interfaceClazz.equals(interfaceType.getRawClass())) {
					resolvableType = interfaceType;
					break;
				}
			}
			if (resolvableType == null) {
				currentType = currentType.getSuperType();
			}
			else {
				ResolvableType[] generics = resolvableType.getGenerics();
				ResolvableType generic = generics[index];
				Class<?> resolvedParameter = generic.resolve();
				if (resolvedParameter != null) {
					bindableType = resolvedParameter;
				}
				else {
					bindableType = Object.class;
				}
			}
		}
		if (bindableType == null) {
			throw new IllegalStateException("Cannot find parameter for " + evaluatedClass.getName());
		}
		return bindableType;
	}
}
