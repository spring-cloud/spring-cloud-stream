/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.config;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * NOT INTENDED FOR PUBLIC USE! Was primarily created to address GH-1359.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 * @see BinderProperties
 * @see ProducerProperties
 * @see ConsumerProperties
 */
public interface MergableProperties {

	/**
	 * A variation of {@link BeanUtils#copyProperties(Object, Object)} specifically designed to copy properties using the following rule:
	 * <p>
	 * - If source property is null then override with the same from mergable.
	 * - If source property is an array and it is empty then override with same from mergable.
	 * - If source property is mergable then merge.
	 */
	default void merge(MergableProperties mergable) {
		if (mergable == null) {
			return;
		}
		for (PropertyDescriptor targetPd : BeanUtils.getPropertyDescriptors(mergable.getClass())) {
			Method writeMethod = targetPd.getWriteMethod();
			if (writeMethod != null) {
				PropertyDescriptor sourcePd = BeanUtils.getPropertyDescriptor(this.getClass(), targetPd.getName());
				if (sourcePd != null) {
					Method readMethod = sourcePd.getReadMethod();
					if (readMethod != null &&
							ClassUtils.isAssignable(writeMethod.getParameterTypes()[0], readMethod.getReturnType())) {
						try {
							if (!Modifier.isPublic(readMethod.getDeclaringClass().getModifiers())) {
								readMethod.setAccessible(true);
							}
							Object value = readMethod.invoke(this);
							if (value != null) {
								if (value instanceof MergableProperties) {
									((MergableProperties) value).merge((MergableProperties) readMethod.invoke(mergable));
								}
								else {
									Object v = readMethod.invoke(mergable);
									if (v == null || (ObjectUtils.isArray(v) && ObjectUtils.isEmpty(v)) ||
											isEmptyMapAtDestination(v)) {
										if (!Modifier.isPublic(writeMethod.getDeclaringClass().getModifiers())) {
											writeMethod.setAccessible(true);
										}
										writeMethod.invoke(mergable, value);
									}
									else if (isMergableByMap(v)) {
										handleMapMerging(value, v);
									}
									else if (!ObjectUtils.nullSafeEquals(v, value)) {
										Object obj = BeanUtils.instantiateClass(this.getClass());
										Object defaultValue = readMethod.invoke(obj);
										if (ObjectUtils.nullSafeEquals(v, defaultValue)) {
											writeMethod.invoke(mergable, value);
										}

									}
								}
							}
						}
						catch (Throwable ex) {
							throw new FatalBeanException(
									"Could not copy property '" + targetPd.getName() + "' from source to target", ex);
						}
					}
				}
			}
		}
	}

	default boolean isEmptyMapAtDestination(Object v) {
		return Map.class.isAssignableFrom(v.getClass()) && CollectionUtils.isEmpty((Map<?,?>) v);
	}

	default boolean isMergableByMap(Object v) {
		return (Map.class.isAssignableFrom(v.getClass()) && !CollectionUtils.isEmpty((Map<?,?>) v));
	}

	@SuppressWarnings("unchecked")
	default void handleMapMerging(Object value, Object v) {
		if (value instanceof Map) {
			Map<Object, Object> sourceMap = (Map<Object, Object>) value;
			for (Object key : sourceMap.keySet()) {
				Map<Object, Object> targetMap = (Map<Object, Object>) v;
				if (!targetMap.containsKey(key)) {
					targetMap.put(key, sourceMap.get(key));
				}
			}
		}
	}

	default void copyProperties(Object source, Object target) throws BeansException {
		// noop
	}
}
