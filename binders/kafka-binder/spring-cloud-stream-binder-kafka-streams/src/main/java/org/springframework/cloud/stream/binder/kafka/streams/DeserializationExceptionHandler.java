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

package org.springframework.cloud.stream.binder.kafka.streams;

/**
 * Enumeration for various {@link org.apache.kafka.streams.errors.DeserializationExceptionHandler} types.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
public enum DeserializationExceptionHandler {

	/**
	 * Deserialization error handler with log and continue.
	 * See {@link org.apache.kafka.streams.errors.LogAndContinueExceptionHandler}
	 */
	logAndContinue,
	/**
	 * Deserialization error handler with log and fail.
	 * See {@link org.apache.kafka.streams.errors.LogAndFailExceptionHandler}
	 */
	logAndFail,
	/**
	 * Deserialization error handler with DLQ send.
	 * See {@link org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler}
	 */
	sendToDlq,
	/**
	 * Deserialization error handler that silently skips the error and continue.
	 */
	skipAndContinue;

}
