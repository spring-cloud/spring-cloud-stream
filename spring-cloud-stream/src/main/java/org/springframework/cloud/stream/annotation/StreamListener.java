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

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be a listener to an input component declared through {@link EnableBinding}
 * (e.g. a channel).
 *
 * Annotated methods are allowed to have flexible signatures, as described by {@link MessageMapping}.
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
 * @see {@link MessageMapping}
 * @see {@link EnableBinding}
 * @see {@link org.springframework.messaging.handler.annotation.SendTo}
 * @author Marius Bogoevici
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface StreamListener {

	/**
	 * The name of the bound component (e.g. channel) that the method subscribes to.
	 *
	 */
	String value() default "";

}
