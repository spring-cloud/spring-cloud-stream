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

package org.springframework.cloud.stream.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.config.AggregateBuilderConfiguration;
import org.springframework.cloud.stream.config.ChannelBindingAdapterConfiguration;
import org.springframework.cloud.stream.config.ModuleRegistrar;
import org.springframework.cloud.stream.config.ChannelBindingAdapterRunner;
import org.springframework.cloud.stream.binder.rabbit.config.RabbitServiceAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.MessageEndpoint;

/**
 * Annotation that identifies a class as a module.
 *
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author David Turanski
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Configuration
@MessageEndpoint
@Import({
	ChannelBindingAdapterConfiguration.class, ChannelBindingAdapterRunner.class,
	AggregateBuilderConfiguration.class, ModuleRegistrar.class})
public @interface EnableModule {

	Class<?>[] value() default {};

}
