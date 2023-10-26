/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.aot.AotDetector;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.annotation.UserConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.aot.ApplicationContextAotGenerator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.test.tools.CompileWithForkedClassLoader;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.javapoet.ClassName;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link BinderChildContextInitializer}.
 *
 * @author Chris Bono
 */
@ExtendWith(OutputCaptureExtension.class)
@Disabled
class BinderChildContextInitializerTests {

	private static final LogAccessor LOG = new LogAccessor(BinderChildContextInitializerTests.class);

	@Test
	@CompileWithForkedClassLoader
	void shouldStartDefaultBinderChildContextFromAotContributions(CapturedOutput output) {

		// Test description:
		// -----------------------
		// Use context runner to create a boostrap context that we can then pass into AOT processor.
		// The AOT processor will then generate the ACI for the default binder (no user declared binders).
		// We then initialize a fresh app context using the generated ACI and verify the expected output.

		ApplicationContextRunner contextRunner = new ApplicationContextRunner()
				.withConfiguration(AutoConfigurations.of(BinderFactoryAutoConfiguration.class,
						BindingServiceConfiguration.class, FunctionConfiguration.class))
				.withPropertyValues("logging.level.org.springframework", "DEBUG")
				.withInitializer(new ConfigDataApplicationContextInitializer())
				.withConfiguration(UserConfigurations.of(TestFooBinderAppConfiguration.class));
		contextRunner.prepare(context -> {
			TestGenerationContext generationContext = new TestGenerationContext(TestTarget.class);
			ClassName className = new ApplicationContextAotGenerator().processAheadOfTime(
					(GenericApplicationContext) context.getSourceApplicationContext(), generationContext);
			generationContext.writeGeneratedContent();
			TestCompiler compiler = TestCompiler.forSystem();
			compiler.with(generationContext).compile(compiled -> {
				// Initialize the context w/ the generated ACI
				GenericApplicationContext freshApplicationContext = new GenericApplicationContext();
				System.out.println("*** Before ACI init -> OUTPUT: " + output.getAll());
				ApplicationContextInitializer<GenericApplicationContext> initializer = compiled
						.getInstance(ApplicationContextInitializer.class, className.toString());
				initializer.initialize(freshApplicationContext);
				System.out.println("*** After ACI init -> OUTPUT: " + output.getAll());
				assertThat(output).contains("Beginning AOT processing for binder child contexts");
				assertThat(output).contains("Pre-creating binder child context (AOT) for mock");
				assertThat(output).contains("Generating AOT child context initializer for mock");
				assertThat(output).contains("Refreshing mock_context");

				// Refresh the initialized context and verify the binder child contexts are used
				TestPropertyValues.of(AotDetector.AOT_ENABLED + "=true")
						.applyToSystemProperties(freshApplicationContext::refresh);
				assertThat(output).contains("Replacing instance w/ one that uses child context initializers");
				assertThat(output).contains("Setting binder child context initializers on binder factory");

				// Make sure we can get the binders
				DefaultBinderFactory binderFactory = freshApplicationContext.getBean(DefaultBinderFactory.class);
				Binder<MessageChannel, ?, ?> mockBinder = binderFactory.getBinder("mock", MessageChannel.class);
				assertThat(mockBinder).isNotNull();
				assertThat(output).contains("Caching the binder: mock");

				// no default or name given - uses single available binder
				assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(mockBinder);
				assertThat(output).contains("No specific name or default given - using single available child initializer 'mock'");

				assertThatIllegalStateException().isThrownBy(
								() -> binderFactory.getBinder("mockBinder1", MessageChannel.class))
						.withMessageContaining("Requested binder 'mockBinder1' did not match available binders");

				binderFactory.setDefaultBinder("mock");
				assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(mockBinder);
			});
		});
	}

	@Test
	@CompileWithForkedClassLoader
	@SuppressWarnings("unchecked")
	void shouldStartDeclardBinderChildContextsFromAotContributions(CapturedOutput output) {

		// Test description:
		// -----------------------
		// Use context runner to create a boostrap context that we can then pass into AOT processor.
		// The AOT processor will then generate the ACI for each binder child context defined in the application.yml.
		// We then initialize a fresh app context using the generated ACIs and verify the expected output.
		ApplicationContextRunner contextRunner = new ApplicationContextRunner()
				.withConfiguration(AutoConfigurations.of(BinderFactoryAutoConfiguration.class,
						BindingServiceConfiguration.class, FunctionConfiguration.class))
				.withInitializer(new ConfigDataApplicationContextInitializer())
				.withPropertyValues("spring.config.location=classpath:binder-aot-test/")
				.withConfiguration(UserConfigurations.of(TestFooBinderAppConfiguration.class));

		contextRunner.prepare(context -> {
			TestGenerationContext generationContext = new TestGenerationContext(TestTarget.class);
			ClassName className = new ApplicationContextAotGenerator().processAheadOfTime(
					(GenericApplicationContext) context.getSourceApplicationContext(), generationContext);
			generationContext.writeGeneratedContent();
			TestCompiler compiler = TestCompiler.forSystem();
			compiler.with(generationContext).compile(compiled -> {
				// Initialize the context w/ the generated ACI
				GenericApplicationContext freshApplicationContext = new GenericApplicationContext();
				System.out.println("*** Before ACI init -> OUTPUT: " + output.getAll());
				ApplicationContextInitializer<GenericApplicationContext> initializer = compiled
						.getInstance(ApplicationContextInitializer.class, className.toString());
				initializer.initialize(freshApplicationContext);
				System.out.println("*** After ACI init -> OUTPUT: " + output.getAll());
				assertThat(output).contains("Beginning AOT processing for binder child contexts");
				assertThat(output).contains("Pre-creating binder child context (AOT) for mockBinder2");
				assertThat(output).contains("Pre-creating binder child context (AOT) for mockBinder1");
				assertThat(output).contains("Generating AOT child context initializer for mockBinder2");
				assertThat(output).contains("Refreshing mockBinder2_context");
				assertThat(output).contains("Generating AOT child context initializer for mockBinder1");
				assertThat(output).contains("Refreshing mockBinder1_context");

				// Refresh the initialized context and verify the binder child contexts are used
				TestPropertyValues.of(AotDetector.AOT_ENABLED + "=true")
						.applyToSystemProperties(freshApplicationContext::refresh);

				assertThat(output).contains("Replacing instance w/ one that uses child context initializers");
				assertThat(output).contains("Setting binder child context initializers on binder factory");

				// Make sure we can get the binders
				DefaultBinderFactory binderFactory = freshApplicationContext.getBean(DefaultBinderFactory.class);
				assertThat(binderFactory.getBinder("mockBinder1", MessageChannel.class)).isNotNull();
				assertThat(output).contains("Caching the binder: mockBinder1");

				Binder mockBinder2 = binderFactory.getBinder("mockBinder2", MessageChannel.class);
				assertThat(mockBinder2).isNotNull();
				assertThat(output).contains("Caching the binder: mockBinder2");

				assertThatIllegalStateException().isThrownBy(
						() -> binderFactory.getBinder("mockBinder3", MessageChannel.class))
						.withMessageContaining("Requested binder 'mockBinder3' did not match available binders");

				assertThatIllegalStateException().isThrownBy(
								() -> binderFactory.getBinder(null, MessageChannel.class))
						.withMessageContaining("No specific name or default given - can't determine which binder to use");

				binderFactory.setDefaultBinder("mockBinder2");
				assertThat(binderFactory.getBinder(null, MessageChannel.class)).isSameAs(mockBinder2);
			});
		});
	}

	static class TestTarget {
	}

	@EnableAutoConfiguration
	@Configuration(proxyBeanMethods = false)
	static class TestFooBinderAppConfiguration {

		@Bean
		GenericConversionService integrationConversionService() {
			return mock(GenericConversionService.class);
		}

		@Bean
		Supplier<String> fooSource() {
			return () -> "foo-" + System.currentTimeMillis();
		}

		@Bean
		Consumer<String> fooSink() {
			return (foo) -> LOG.info("*** FOO: " + foo);
		}
	}

}
