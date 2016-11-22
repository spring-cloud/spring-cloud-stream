/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.reactive;

import rx.Observable;
import rx.Single;

/**
 * Used for {@link org.springframework.cloud.stream.annotation.StreamListener} arguments
 * annotated with {@link org.springframework.cloud.stream.annotation.Output}.
 *
 * @author Marius Bogoevici
 */
public interface ObservableSender {

	/**
	 * Streams the {@link Observable} through the binding target corresponding to the
	 * {@link org.springframework.cloud.stream.annotation.Output} annotation of the
	 * argument.
	 *
	 * @param observable an {@link Observable} that will be streamed through the bound
	 * element
	 * @return a {@link Single} representing the result of an operation
	 */
	Single<Void> send(Observable<?> observable);
}
