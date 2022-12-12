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

package org.springframework.cloud.stream.function;

import org.springframework.lang.Nullable;
import org.springframework.util.MimeType;

/**
 * Basic contract for {@link StreamBridge} operations.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 * @since 4.0.0
 */
public interface StreamOperations {

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using default content type to deal with output type conversion (if necessary).
	 * For typical cases `bindingName` is configured using 'spring.cloud.stream.source' property.
	 * However, this operation also supports sending to truly dynamic destinations. This means if the name
	 * provided via 'bindingName' does not have a corresponding binding such name will be
	 * treated as dynamic destination.<br>
	 * Will use default binder. For specific binder type see {@link #send(String, String, Object)} and
	 * {@link #send(String, String, Object, MimeType)} methods.
	 *
	 * @param bindingName the name of the output binding. That said it requires a bit of clarification.
	 *        When using send("foo"...), the 'foo' typically represents the binding name.
	 *        However, if such binding does not exist, the new binding will be created
	 *        to support dynamic destinations.
	 * @param data the data to send
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	boolean send(String bindingName, Object data);

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using the content type specified by the 'outputContentType' argument to deal
	 * with output type conversion (if necessary).
	 * For typical cases `bindingName` is configured using 'spring.cloud.stream.source' property.
	 * However, this operation also supports sending to truly dynamic destinations. This means if the name
	 * provided via 'bindingName' does not have a corresponding binding such name will be
	 * treated as dynamic destination.<br>
	 * Will use default binder. For specific binder type see {@link #send(String, String, Object)} and
	 * {@link #send(String, String, Object, MimeType)} methods.
	 *
	 * @param bindingName the name of the output binding. That said it requires a bit of clarification.
	 * When using bridge.send("foo"...), the 'foo' typically represents the binding name.
	 *        However, if such binding does not exist, the new binding will be created to support
	 *        dynamic destinations.
	 * @param data the data to send
	 * @param outputContentType content type to be used to deal with output type conversion
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	boolean send(String bindingName, Object data, MimeType outputContentType);

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using the content type specified by the 'outputContentType' argument to deal
	 * with output type conversion (if necessary).
	 * For typical cases `bindingName` is configured using 'spring.cloud.stream.source' property.
	 * However, this operation also supports sending to truly dynamic destinations. This means if the name
	 * provided via 'bindingName' does not have a corresponding binding such name will be
	 * treated as dynamic destination.
	 *
	 * @param bindingName the name of the output binding. That said it requires a bit of clarification.
	 *        When using bridge.send("foo"...), the 'foo' typically represents the binding name.
	 *        However, if such binding does not exist, the new binding will be created to support dynamic destinations.
	 * @param binderName the name of the binder to use (e.g., 'kafka', 'rabbit') for cases where multiple
	 *                   binders are used. Can be null.
	 * @param data the data to send
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	boolean send(String bindingName, @Nullable String binderName, Object data);

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using the content type specified by the 'outputContentType' argument to deal
	 * with output type conversion (if necessary).
	 * For typical cases `bindingName` is configured using 'spring.cloud.stream.source' property.
	 * However, this operation also supports sending to truly dynamic destinations. This means if the name
	 * provided via 'bindingName' does not have a corresponding binding such name will be
	 * treated as dynamic destination.
	 *
	 * @param bindingName the name of the output binding. That said it requires a bit of clarification.
	 *        When using bridge.send("foo"...), the 'foo' typically represents the binding name.
	 *        However, if such binding does not exist, the new binding will be created to support dynamic destinations.
	 * @param binderName the name of the binder to use (e.g., 'kafka', 'rabbit') for cases where multiple binders are used. Can be null.
	 * @param data the data to send
	 * @param outputContentType content type to be used to deal with output type conversion
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	boolean send(String bindingName, @Nullable String binderName, Object data, MimeType outputContentType);

}
