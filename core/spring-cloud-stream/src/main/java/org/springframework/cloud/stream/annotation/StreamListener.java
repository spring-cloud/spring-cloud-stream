/*
 * Copyright 2016-2017 the original author or authors.
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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.core.annotation.AliasFor;
import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * <b>NOTE: It is no longer recommended to use StreamListener in favor of functional programming model.
 * It will be deprecated and subsequently removed in the future</b>
 * <br>
 * <br>
 * Annotation that marks a method to be a listener to inputs declared via
 * {@link EnableBinding} (e.g. channels).
 *
 * Annotated methods are allowed to have flexible signatures, which determine how the
 * method is invoked and how their return results are processed. This annotation can be
 * applied for two separate classes of methods.
 *
 * <h3>Declarative mode</h3>
 *
 * A method is considered declarative if all its method parameter types and return type
 * (if not void) are binding targets or conversion targets from binding targets via a
 * registered {@link StreamListenerParameterAdapter}.
 *
 * Only declarative methods can have binding targets or conversion targets as arguments
 * and return type.
 *
 * Declarative methods must specify what inputs and outputs correspond to their arguments
 * and return type, and can do this in one of the following ways.
 *
 * <ul>
 * <li>By using either the {@link Input} or {@link Output} annotation for each of the
 * parameters and the {@link Output} annotation on the method for the return type (if
 * applicable). The use of annotations in this case is mandatory. In this case the
 * {@link StreamListener} annotation must not specify a value.</li>
 * <li>By setting an {@link Input} bound target as the annotation value of
 * {@link StreamListener} and using
 * {@link org.springframework.messaging.handler.annotation.SendTo} on the method for
 * the return type (if applicable). In this case the method must have exactly one
 * parameter, corresponding to an input.
 * </li>
 * </ul>
 *
 * An example of declarative method signature using the former idiom is as follows:
 *
 * <pre class="code">
 * &#064;StreamListener
 * public &#064;Output("joined") Flux&lt;String&gt; join(
 *       &#064;Input("input1") Flux&lt;String&gt; input1,
 *       &#064;Input("input2") Flux&lt;String&gt; input2) {
 *   // ... join the two input streams via functional operators
 * }
 * </pre>
 *
 * An example of declarative method signature using the latter idiom is as follows:
 *
 * <pre class="code">
 * &#064;StreamListener(Processor.INPUT)
 * &#064;SendTo(Processor.OUTPUT)
 * public Flux&lt;String&gt; convert(Flux&lt;String&gt; input) {
 *     return input.map(String::toUppercase);
 * }
 * </pre>
 *
 * Declarative methods are invoked only once, when the context is refreshed.
 *
 * <h3>Individual message handler mode</h3>
 *
 * Non declarative methods are treated as message handler based, and are invoked for each
 * incoming message received from that target. In this case, the method can have a
 * flexible signature, as described by {@link MessageMapping}.
 *
 * If the method returns a {@link org.springframework.messaging.Message}, the result will
 * be automatically sent to a binding target, as follows:
 * <ul>
 * <li>A result of the type {@link org.springframework.messaging.Message} will be sent
 * as-is</li>
 * <li>All other results will become the payload of a
 * {@link org.springframework.messaging.Message}</li>
 * </ul>
 *
 * The output binding target where the return message is sent is determined by consulting
 * in the following order:
 * <ul>
 * <li>The {@link org.springframework.messaging.MessageHeaders} of the resulting
 * message.</li>
 * <li>The value set on the
 * {@link org.springframework.messaging.handler.annotation.SendTo} annotation, if
 * present</li>
 * </ul>
 *
 * An example of individual message handler signature is as follows:
 *
 * <pre class="code">
 * &#064;StreamListener(Processor.INPUT)
 * &#064;SendTo(Processor.OUTPUT)
 * public String convert(String input) {
 * 		return input.toUppercase();
 * }
 * </pre>
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @see MessageMapping
 * @see EnableBinding
 * @see org.springframework.messaging.handler.annotation.SendTo
 *
 * @deprecated as of 3.1 in favor of functional programming model
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Deprecated
public @interface StreamListener {

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor("target")
	String value() default "";

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor("value")
	String target() default "";

	/**
	 * A condition that must be met by all items that are dispatched to this method.
	 * @return a SpEL expression that must evaluate to a {@code boolean} value.
	 */
	String condition() default "";

	/**
	 * When "true" (default), and a {@code @SendTo} annotation is present, copy the
	 * inbound headers to the outbound message (if the header is absent on the outbound
	 * message). Can be an expression ({@code #{...}}) or property placeholder. Must
	 * resolve to a boolean or a string that is parsed by {@code Boolean.parseBoolean()}.
	 * An expression that resolves to {@code null} is interpreted to mean {@code false}.
	 *
	 * The expression is evaluated during application initialization, and not for each
	 * individual message.
	 *
	 * Prior to version 1.3.0, the default value used to be "false" and headers were not
	 * propagated by default.
	 *
	 * Starting with version 1.3.0, the default value is "true".
	 *
	 * @since 1.2.3
	 * @return {@link Boolean} in a String format
	 */
	String copyHeaders() default "true";

}
