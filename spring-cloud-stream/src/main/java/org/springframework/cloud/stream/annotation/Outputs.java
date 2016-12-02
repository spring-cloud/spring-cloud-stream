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
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Indicates that a list of output channels will be created by the framework.
 *
 * @author Laabidi RAISSI
 */

@Qualifier
@Target({ ElementType.FIELD, ElementType.METHOD,
		ElementType.ANNOTATION_TYPE, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface Outputs {

	/**
	 * Specifies the names of the output channels, may be a hard coded list, e.g: <code>"output1,output2"</code>
	 * or a placeholder referencing a config property, e.g: <code>"${my.list.of.strings}"</code>. 
	 * If empty, then the name of the method will be used to create a unique channel
	 * @return
	 */
	String value() default "";

}
