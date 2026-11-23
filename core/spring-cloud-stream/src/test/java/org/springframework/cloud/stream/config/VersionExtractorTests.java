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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link VersionExtractor}.
 *
 * @author Oleg Zhurakousky
 */
@ExtendWith(OutputCaptureExtension.class)
class VersionExtractorTests {

	private static final String BANNER_LOCATION = "spring.banner.location";

	@TempDir
	Path tempDir;

	private final VersionExtractor versionExtractor = new VersionExtractor();

	@BeforeEach
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

	@Test
	void bannerIsContributedByDefault() {
		MockEnvironment environment = new MockEnvironment();
		this.versionExtractor.postProcessEnvironment(environment, new SpringApplication());

		assertThat(environment.getProperty(BANNER_LOCATION)).isEqualTo(VersionExtractor.BANNER_LOCATION);
		assertThat(System.getProperty("spring-cloud-stream.version")).isNotNull();
		assertThat(System.getProperty("spring-cloud-function.version")).isNotNull();
	}

	@Test
	void bannerIsNotContributedWhenDisabled() {
		MockEnvironment environment = new MockEnvironment().withProperty(VersionExtractor.BANNER_ENABLED_PROPERTY,
				"false");
		this.versionExtractor.postProcessEnvironment(environment, new SpringApplication());

		assertNoBanner(environment);
	}

	@Test
	void bannerIsNotContributedWhenBannerModeIsOff() {
		MockEnvironment environment = new MockEnvironment().withProperty("spring.main.banner-mode", "off");
		this.versionExtractor.postProcessEnvironment(environment, new SpringApplication());

		assertNoBanner(environment);
	}

	@Test
	void bannerLocationConfiguredByApplicationIsNotOverridden() {
		MockEnvironment environment = new MockEnvironment().withProperty(BANNER_LOCATION, "classpath:my-banner.txt");
		this.versionExtractor.postProcessEnvironment(environment, new SpringApplication());

		assertThat(environment.getProperty(BANNER_LOCATION)).isEqualTo("classpath:my-banner.txt");
		assertThat(System.getProperty("spring-cloud-stream.version")).isNull();
	}

	@Test
	void applicationBannerTakesPrecedence() throws Exception {
		Files.writeString(this.tempDir.resolve("banner.txt"), "MY APPLICATION BANNER");
		MockEnvironment environment = new MockEnvironment();
		this.versionExtractor.postProcessEnvironment(environment, applicationWithClasspath());

		assertNoBanner(environment);
	}

	@Test
	void applicationImageBannerTakesPrecedence() throws Exception {
		Files.write(this.tempDir.resolve("banner.png"), new byte[] { 1 });
		MockEnvironment environment = new MockEnvironment();
		this.versionExtractor.postProcessEnvironment(environment, applicationWithClasspath());

		assertNoBanner(environment);
	}

	@Test
	void applicationPrintsOwnBannerInsteadOfSpringCloudStreamOne(CapturedOutput output) throws Exception {
		Files.writeString(this.tempDir.resolve("banner.txt"), "MY APPLICATION BANNER");

		runApplication(applicationWithClasspath());

		assertThat(output).contains("MY APPLICATION BANNER").doesNotContain(":: Spring Cloud Stream ::");
	}

	@Test
	void springCloudStreamBannerIsPrintedWhenApplicationHasNone(CapturedOutput output) {
		runApplication(new SpringApplication(EmptyConfiguration.class));

		assertThat(output).contains(":: Spring Cloud Stream ::").contains(":: Spring Cloud Function ::");
	}

	@Test
	void springCloudStreamBannerCanBeDisabled(CapturedOutput output) {
		SpringApplication application = new SpringApplication(EmptyConfiguration.class);
		application.setDefaultProperties(Map.of(VersionExtractor.BANNER_ENABLED_PROPERTY, "false"));
		runApplication(application);

		assertThat(output).doesNotContain(":: Spring Cloud Stream ::").contains(":: Spring Boot ::");
	}

	private SpringApplication applicationWithClasspath() throws Exception {
		URLClassLoader classLoader = new URLClassLoader(new URL[] { this.tempDir.toUri().toURL() },
				getClass().getClassLoader());
		SpringApplication application = new SpringApplication(EmptyConfiguration.class);
		application.setResourceLoader(new DefaultResourceLoader(classLoader));
		return application;
	}

	private void runApplication(SpringApplication application) {
		application.setWebApplicationType(WebApplicationType.NONE);
		application.setRegisterShutdownHook(false);
		application.run("--spring.main.web-application-type=none", "--spring.cloud.function.definition=")
			.close();
	}

	private void assertNoBanner(MockEnvironment environment) {
		assertThat(environment.containsProperty(BANNER_LOCATION)).isFalse();
		assertThat(System.getProperty("spring-cloud-stream.version")).isNull();
		assertThat(System.getProperty("spring-cloud-function.version")).isNull();
	}

	@Configuration(proxyBeanMethods = false)
	static class EmptyConfiguration {

	}

}
