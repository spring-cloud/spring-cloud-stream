/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.annotationprocessor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark a field in an {@literal @ConfigurationProperties} annotated class, so that an alternative synthetic property
 * be taken into account to populate the value of the annotated field. This is typically used as a way to allow
 * literal String values to be used for configuration properties that expect a SpEL expression.
 *
 * <p>
 *     Here is an example. Assume you want a module to have a 'name' property, but also allow that named to be
 *     dervied from a SpEL expression (resolved against the incoming message). You could add fields for both
 *     {@literal name} and {@literal nameExpression} in your {@literal @ConfigurationProperties} class, and have an
 *     {@literal if} statement to decide which one to use, <i>etc.</i>
 * </p>
 * <p>
 *     As an alternative, do the following:
 *<pre>
 *{@code
 * &#47;** This is the documentation for the nameExpression property. *&#47;
 *   &#64;ImplicitExpression("This is the doco for the name property")
 *   private Expression nameExpression;
 *}</pre>
 * </p>
 *
 * <p>
 *     This will document a synthetic {@literal name} property, that can be properly resolved
 *     at runtime thanks to the ImplicitExpressionConfiguration. Then, passing <i>e.g.</i>
 *     {@literal --name=foo} would behave as if one had passed {@literal --nameExpression='foo'}.
 * </p>
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
@Documented
public @interface ImplicitExpression {

	/**
	 * The type of the alternate property. Influences how literal values for SpEL expressions are
	 * created.
	 */
	Class<?> type() default String.class;

	/**
	 * A simple one-line documentation for the alternate property.
	 */
	String value();

}
