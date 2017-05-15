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

package org.springframework.cloud.stream.annotation.rxjava;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Import;

/**
 * Annotation that identifies the class as RxJava processor module. The class that has
 * {@link EnableRxJavaProcessor} annotated is expected to provide a bean that implements
 * {@link RxJavaProcessor}.
 *
 * @author Ilayaperumal Gopinathan
 * @deprecated in favor of
 * {@link org.springframework.cloud.stream.annotation.StreamListener} with reactive types
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@EnableBinding(Processor.class)
@Import(RxJavaProcessorConfiguration.class)
@Deprecated
public @interface EnableRxJavaProcessor {
}
