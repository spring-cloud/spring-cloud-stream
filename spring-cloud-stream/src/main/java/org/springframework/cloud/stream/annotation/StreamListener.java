/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be a listener to the inputs declared through {@link EnableBinding}
 * (e.g. channels).
 *
 * Annotated methods are allowed to have flexible signatures, which determine how
 * the method is invoked and how their return results are processed. This annotation
 * can be applied for two separate classes of methods.
 *
 * <h3>Individual message handler mode</h3>
 *
 * Methods where the annotation has a value, are treated as message handlers, and are invoked for each
 * incoming message received from that target. In this case, the
 * method can have a flexible signature, as described by {@link MessageMapping}.
 * The value must be the name of an {@link Input} bound target.
 *
 * If the method returns a {@link org.springframework.messaging.Message}, the result will be automatically sent
 * to a channel, as follows:
 * <ul>
 * <li>A result of the type {@link org.springframework.messaging.Message} will be sent as-is</li>
 * <li>All other results will become the payload of a {@link org.springframework.messaging.Message}</li>
 * </ul>
 *
 * The target channel of the return message is determined by consulting in the following order:
 * <ul>
 * <li>The {@link org.springframework.messaging.MessageHeaders} of the resulting message.</li>
 * <li>The value set on the {@link org.springframework.messaging.handler.annotation.SendTo} annotation, if present</li>
 * </ul>
 *
 * <h3>Declarative mode</h3>
 *
 * If the annotation has an empty value (the default), the method is a declarative
 * pipeline definition and will be invoked once, when the application starts.
 * All parameters must be annotated with either {@link Input} or {@link Output} and can
 * be either bound elements (e.g. channels) or conversion targets from bound elements
 * via a registered {@link StreamListenerParameterAdapter}.
 * @author Marius Bogoevici
 * @see {@link MessageMapping}
 * @see {@link EnableBinding}
 * @see {@link org.springframework.messaging.handler.annotation.SendTo}
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface StreamListener {

	/**
	 * The name of the bound component (e.g. channel) that the method subscribes to.
	 */
	String value() default "";

}
