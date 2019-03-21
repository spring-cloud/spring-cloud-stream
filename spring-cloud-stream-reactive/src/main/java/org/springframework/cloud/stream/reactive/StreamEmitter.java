/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

/**
 * Method level annotation that marks a method to be an emitter to outputs declared via
 * {@link EnableBinding} (e.g. channels).
 *
 * This annotation is intended to be used in a Spring Cloud Stream application that
 * requires a source to write to one or more {@link Output}s using the reactive paradigm.
 *
 * No {@link Input}s are allowed on a method that is annotated with StreamEmitter.
 *
 * Depending on how the method is structured, there are some flexibility in how the
 * {@link Output} may be used.
 *
 * Here are some supported usage patterns:
 *
 * A StreamEmitter method that has a return type cannot take any method parameters.
 *
 * <pre class="code">
 * &#064;StreamEmitter
 * &#064;Output(Source.OUTPUT)
 * public Flux&lt;String&gt; emit() {
 * 	return Flux.intervalMillis(1000)
 * 		.map(l -> "Hello World!!");
 * }
 * </pre>
 *
 * The following examples show how a void return type can be used on a method with
 * StreamEmitter and how the method signatures could be used in a flexible manner.
 *
 * <pre class="code">
 * &#064;StreamEmitter
 * public void emit(&#064;Output(Source.OUTPUT) FluxSender output) {
 * 	output.send(Flux.intervalMillis(1000)
 *		.map(l -> "Hello World!!"));
 * }
 * </pre>
 *
 * <pre class="code">
 * &#064;StreamEmitter
 * &#064;Output(Source.OUTPUT)
 * public void emit(FluxSender output) {
 * 	output.send(Flux.intervalMillis(1000)
 *		.map(l -> "Hello World!!"));
 * }
 * </pre>
 *
 * <pre class="code">
 * &#064;StreamEmitter
 * public void emit(&#064;Output("OUTPUT1") FluxSender output1,
 *					&#064;Output("OUTPUT2") FluxSender output2,
 *					&#064;Output("OUTPUT3)" FluxSender output3) {
 *	output1.send(Flux.intervalMillis(1000)
 *		.map(l -> "Hello World!!"));
 *	output2.send(Flux.intervalMillis(1000)
 *		.map(l -> "Hello World!!"));
 *	output3.send(Flux.intervalMillis(1000)
 *		.map(l -> "Hello World!!"));
 *	}
 *</pre>
 *
 * @author Soby Chacko
 *
 * @since 1.3.0
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamEmitter {

}
