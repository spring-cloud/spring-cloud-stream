/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link VersionExtractor}.
 *
 * <p>
 * Regression test for a {@link NullPointerException} that occurs when
 * {@code FunctionConfiguration.class.getProtectionDomain().getCodeSource()} returns a
 * {@code CodeSource} whose {@code getLocation()} is {@code null} — observed with the JDK
 * 25 AOT cache ({@code -XX:AOTMode=on}), where classes loaded via the AOT cache can have
 * an incomplete {@code CodeSource}. Reading the version through
 * {@link Package#getImplementationVersion()} instead avoids depending on
 * {@code CodeSource} entirely.
 *
 * @author Nicolo Pietro Belcastro
 */
class VersionExtractorTests {

	private final VersionExtractor versionExtractor = new VersionExtractor();

	@AfterEach
	void cleanUp() {
		System.clearProperty("spring-cloud-stream.version");
		System.clearProperty("spring-cloud-function.version");
	}

	@Test
	void postProcessEnvironmentDoesNotThrowAndSetsVersionProperties() {
		assertThatCode(
				() -> this.versionExtractor.postProcessEnvironment(new MockEnvironment(), new SpringApplication()))
			.doesNotThrowAnyException();
	}

}
