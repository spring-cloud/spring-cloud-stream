/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.annotation.rxjava;

import rx.Observable;

/**
 * Marker interface that RxJava processor module uses to provide the implementation bean.
 *
 * @author Mark Pollack
 * @author Ilayaperumal Gopinathan
 * @deprecated in favor of
 * {@link org.springframework.cloud.stream.annotation.StreamListener} with reactive types
 */
@Deprecated
public interface RxJavaProcessor<I, O> {

	Observable<O> process(Observable<I> input);
}
