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

package org.springframework.cloud.stream.binder.rabbit.deployer;

import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


/**
 *
 * @author Oleg Zhurakousky
 * @since 3.1.2
 *
 */
@SpringBootApplication
public class RabbitDeployer {

	public static void main(String[] args) {
		SpringApplication.run(RabbitDeployer.class,

				//target/it/bootjar/target/bootjar-1.0.0.RELEASE-exec.jar
				"--spring.cloud.function.definition=reverseFunction;echo",
				"--spring.cloud.function.location=/Users/ozhurakousky/dev/repo/spring-cloud-function/spring-cloud-function-deployer/target/it/bootjar/target/bootjar-1.0.0.RELEASE-exec.jar",
				"--spring.cloud.function.function-class=function.example.ReverseFunction");
	}


	@Bean
	public Function<String, String> echo() {
		return v -> v;
	}

}
