/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingBeansRegistrar;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;

/**
 * Enables the binding of targets annotated with {@link Input} and {@link Output} to a
 * broker, according to the list of interfaces passed as value to the annotation.
 *
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Soby Chacko
 *
 * @deprecated as of 3.1 in favor of functional programming model
 */
@Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Configuration(proxyBeanMethods = false)
@Import({ BindingBeansRegistrar.class, BinderFactoryAutoConfiguration.class })
@EnableIntegration
@Deprecated
public @interface EnableBinding {

	/**
	 * A list of interfaces having methods annotated with {@link Input} and/or
	 * {@link Output} to indicate binding targets.
	 * @return list of interfaces
	 */
	Class<?>[] value() default {};

}
