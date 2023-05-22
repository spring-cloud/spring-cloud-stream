/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests for {@link KafkaStreamsBinderUtils} functions
 *
 * @author James Forward
 */
class KafkaStreamsBinderUtilsTest {

	@Test
	void testDeriveFunctionUnitsWithVariedSpacings() {
		final String definition = "firstFunction; secondFunction;   thirdFunction;fourthFunction";

		final String[] functionUnits = KafkaStreamsBinderUtils.deriveFunctionUnits(definition);

		assertAll(
			() -> assertThat(functionUnits.length).isEqualTo(4),
			() -> assertThat(functionUnits[0]).isEqualTo("firstFunction"),
			() -> assertThat(functionUnits[1]).isEqualTo("secondFunction"),
			() -> assertThat(functionUnits[2]).isEqualTo("thirdFunction"),
			() -> assertThat(functionUnits[3]).isEqualTo("fourthFunction")
		);
	}
}
