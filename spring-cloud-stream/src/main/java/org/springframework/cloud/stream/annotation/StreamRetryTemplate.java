/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.support.RetryTemplate;

/**
 * Marker to tag an instance of {@link RetryTemplate} to be used by the binder.
 * This annotation is also a @Bean to simplify configuration (see below)
 *
 * <pre class="code">
 * &#064;StreamRetryTemplate
 * public RetryTemplate myRetryTemplate() {
 *    return new RetryTemplate();
 * }
 * </pre>
 *
 * @author Oleg Zhurakousky
 * @since 2.1
 */

@Target({ElementType.FIELD,ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Bean
@Qualifier
public @interface StreamRetryTemplate {

}
