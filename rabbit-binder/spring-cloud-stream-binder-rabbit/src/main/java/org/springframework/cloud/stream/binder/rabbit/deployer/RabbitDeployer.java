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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * Main application class to trigger the deployment of Java Functions based
 * user application with RabitMQ bindings.
 * <br>
 * <br>
 * It is based on Spring Cloud Function
 * <a href="https://docs.spring.io/spring-cloud-function/docs/3.1.3/reference/html/spring-cloud-function.html#_deploying_a_packaged_function">deploy feature</a>
 *
 * <br>
 * All you need to do is to provide few additional properties to assist Spring Cloud Function deployer.
 * <br>
 * For example:
 * <br>
 * <pre class="code">
 *
 * java -jar spring-cloud-stream-binder-rabbit-deployer/target/spring-cloud-stream-binder-rabbit-deployer-3.2.0-SNAPSHOT.jar\
 * 		 	--spring.cloud.function.location=/foo/bar/simplestjar-1.0.0.RELEASE.jar
 * 			--spring.cloud.function.function-class=function.example.UpperCaseFunction
 *
 * </pre>
 * The only required property is 'spring.cloud.function.location'. Every other property is optional and is based on the style of user application.
 * For more details on the supported styles of user application please refer to
 * <a href="https://docs.spring.io/spring-cloud-function/docs/3.1.3/reference/html/spring-cloud-function.html#_deploying_a_packaged_function">
 * Deploying a Packaged Function</a> section of Spring Cloud Function.
 *
 * @author Oleg Zhurakousky
 * @since 3.1.2
 *
 */
@SpringBootApplication
public class RabbitDeployer {

	public static void main(String[] args) {
		SpringApplication.run(RabbitDeployer.class, args);
	}

}
