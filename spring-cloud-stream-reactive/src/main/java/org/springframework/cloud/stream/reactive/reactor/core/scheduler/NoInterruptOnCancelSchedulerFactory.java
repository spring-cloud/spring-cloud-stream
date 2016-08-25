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

package org.springframework.cloud.stream.reactive.reactor.core.scheduler;

import java.util.concurrent.ThreadFactory;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;

/**
 * {@link reactor.core.scheduler.Schedulers.Factory} implementation that pulls in
 * {@link reactor.core.scheduler.Scheduler} implementations with fixes for
 * <a href="https://github.com/reactor/reactor-core/issues/159"/>.
 *
 * This is a temporary solution, until a Reactor release is available.
 *
 * @author Marius Bogoevici
 */
public class NoInterruptOnCancelSchedulerFactory implements Schedulers.Factory {

	@Override
	public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
		return new ElasticScheduler(threadFactory, ttlSeconds);
	}

	@Override
	public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
		return new ParallelScheduler(parallelism, threadFactory);
	}

	@Override
	public Scheduler newSingle(ThreadFactory threadFactory) {
		return new SingleScheduler(threadFactory);
	}

	@Override
	public TimedScheduler newTimer(ThreadFactory threadFactory) {
		return new SingleTimedScheduler(threadFactory);
	}
}
