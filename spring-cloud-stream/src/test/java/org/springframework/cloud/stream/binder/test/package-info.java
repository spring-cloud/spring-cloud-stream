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

/**
 * Provides test channel binder and supporting classes
 *
 * THe test binder is backed by Spring Integration framework and is not intended for uses
 * outside of local testing.
 *
 * The test binder implementation -
 * {@link org.springframework.cloud.stream.binder.test.TestChannelBinder} The test binder
 * configuration -
 * {@link org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration} The
 * example that shows how to use it -
 * {@link org.springframework.cloud.stream.binder.test.SampleStreamApp}
 *
 */
package org.springframework.cloud.stream.binder.test;
