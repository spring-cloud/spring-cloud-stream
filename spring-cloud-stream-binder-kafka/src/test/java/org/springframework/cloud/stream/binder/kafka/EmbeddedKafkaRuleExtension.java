/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class EmbeddedKafkaRuleExtension extends EmbeddedKafkaRule implements BeforeEachCallback, AfterEachCallback {

	public EmbeddedKafkaRuleExtension(int count, boolean controlledShutdown,
			String... topics) {
		super(count, controlledShutdown, topics);
	}

	public EmbeddedKafkaRuleExtension(int count, boolean controlledShutdown,
			int partitions, String... topics) {
		super(count, controlledShutdown, partitions, topics);
	}

	public EmbeddedKafkaRuleExtension(int count) {
		super(count);
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		this.after();
	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		this.before();
	}
}
