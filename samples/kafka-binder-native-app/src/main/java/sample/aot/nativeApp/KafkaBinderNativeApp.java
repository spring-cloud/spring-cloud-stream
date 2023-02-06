/*
 * Copyright 2023 the original author or authors.
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

package sample.aot.nativeApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class KafkaBinderNativeApp {

	public static void main(String[] args) {
		SpringApplication.run(KafkaBinderNativeApp.class, args);
	}

	@Bean
	public Function<String, String> graalUppercaseFunction() {
		return String::toUpperCase;
	}

	@Bean
	public Consumer<String> graalLoggingConsumer() {
		return s -> {
			System.out.println("++++++Received:" + s);
		};
	}

	@Bean
	public Supplier<String> graalSupplier() {
		final String[] splitWoodchuck = "How much wood could a woodchuck chuck if a woodchuck could chuck wood?"
			.split(" ");
		final AtomicInteger wordsIndex = new AtomicInteger(0);
		return () -> {
			int wordIndex = wordsIndex.getAndAccumulate(splitWoodchuck.length,
				(curIndex, numWords) -> curIndex < numWords - 1 ? curIndex + 1 : 0);
			return splitWoodchuck[wordIndex];
		};
	}

}
