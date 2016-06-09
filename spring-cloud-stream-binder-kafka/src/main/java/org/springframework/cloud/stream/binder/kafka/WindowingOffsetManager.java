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

package org.springframework.cloud.stream.binder.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.GroupedObservable;
import rx.observables.MathObservable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.listener.OffsetManager;
import org.springframework.util.Assert;

/**
 * An {@link OffsetManager} that aggregates writes over a time or count window, using an underlying delegate to
 * do the actual operations. Its purpose is to reduce the performance impact of writing operations
 * wherever this is desirable.
 *
 * Either a time window or a number of writes can be specified, but not both.
 *
 * @author Marius Bogoevici
 */
public class WindowingOffsetManager implements OffsetManager, InitializingBean, DisposableBean {

	private final CreatePartitionAndOffsetFunction createPartitionAndOffsetFunction = new CreatePartitionAndOffsetFunction();

	private final GetOffsetFunction getOffsetFunction = new GetOffsetFunction();

	private final ComputeMaximumOffsetByPartitionFunction findHighestOffsetInPartitionGroup = new ComputeMaximumOffsetByPartitionFunction();

	private final GetPartitionFunction getPartition = new GetPartitionFunction();

	private final FindHighestOffsetsByPartitionFunction findHighestOffsetsByPartition = new FindHighestOffsetsByPartitionFunction();

	private final DelegateUpdateOffsetAction delegateUpdateOffsetAction = new DelegateUpdateOffsetAction();

	private final NotifyObservableClosedAction notifyObservableClosed = new NotifyObservableClosedAction();

	private final OffsetManager delegate;

	private long timespan = 10 * 1000;

	private int count;

	private Subject<PartitionAndOffset, PartitionAndOffset> offsets;

	private Subscription subscription;

	private int shutdownTimeout = 2000;

	private CountDownLatch shutdownLatch;

	public WindowingOffsetManager(OffsetManager offsetManager) {
		this.delegate = offsetManager;
	}

	/**
	 * The timespan for aggregating write operations, before invoking the underlying {@link OffsetManager}.
	 *
	 * @param timespan duration in milliseconds
	 */
	public void setTimespan(long timespan) {
		Assert.isTrue(timespan >= 0, "Timespan must be a positive value");
		this.timespan = timespan;
	}

	/**
	 * How many writes should be aggregated, before invoking the underlying {@link OffsetManager}. Setting this value
	 * to 1 effectively disables windowing.
	 *
	 * @param count number of writes
	 */
	public void setCount(int count) {
		Assert.isTrue(count >= 0, "Count must be a positive value");
		this.count = count;
	}

	/**
	 * The timeout that {@link #close()} and {@link #destroy()} operations will wait for receving a confirmation that the
	 * underlying writes have been processed.
	 *
	 * @param shutdownTimeout duration in milliseconds
	 */
	public void setShutdownTimeout(int shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(timespan > 0 ^ count > 0, "Only one of the timespan or count must be set");
		// create the stream if windowing is set, and count is higher than 1
		if (timespan > 0 || count > 1) {
			offsets = new SerializedSubject<>(PublishSubject.<PartitionAndOffset>create());
			// window by either count or time
			Observable<Observable<PartitionAndOffset>> window =
					timespan > 0 ? offsets.window(timespan, TimeUnit.MILLISECONDS) : offsets.window(count);
			Observable<PartitionAndOffset> maximumOffsetsByWindow = window
					.flatMap(findHighestOffsetsByPartition)
					.doOnCompleted(notifyObservableClosed);
			subscription = maximumOffsetsByWindow.subscribe(delegateUpdateOffsetAction);
		}
		else {
			offsets = null;
		}
	}

	@Override
	public void destroy() throws Exception {
		this.flush();
		this.close();
		if (delegate instanceof DisposableBean) {
			((DisposableBean) delegate).destroy();
		}
	}

	@Override
	public void updateOffset(Partition partition, long offset) {
		if (offsets != null) {
			offsets.onNext(new PartitionAndOffset(partition, offset));
		}
		else {
			delegate.updateOffset(partition, offset);
		}
	}

	@Override
	public long getOffset(Partition partition) {
		return delegate.getOffset(partition);
	}

	@Override
	public void deleteOffset(Partition partition) {
		delegate.deleteOffset(partition);
	}

	@Override
	public void resetOffsets(Collection<Partition> partition) {
		delegate.resetOffsets(partition);
	}

	@Override
	public void close() throws IOException {
		if (offsets != null) {
			shutdownLatch = new CountDownLatch(1);
			offsets.onCompleted();
			try {
				shutdownLatch.await(shutdownTimeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				// ignore
			}
			subscription.unsubscribe();
		}
		delegate.close();
	}

	@Override
	public void flush() throws IOException {
		delegate.flush();
	}

	private final class PartitionAndOffset {

		private final Partition partition;

		private final Long offset;

		private PartitionAndOffset(Partition partition, Long offset) {
			this.partition = partition;
			this.offset = offset;
		}

		public Partition getPartition() {
			return partition;
		}

		public Long getOffset() {
			return offset;
		}
	}

	private class DelegateUpdateOffsetAction implements Action1<PartitionAndOffset> {
		@Override
		public void call(PartitionAndOffset partitionAndOffset) {
			delegate.updateOffset(partitionAndOffset.getPartition(), partitionAndOffset.getOffset());
		}
	}

	private class NotifyObservableClosedAction implements Action0 {
		@Override
		public void call() {
			if (shutdownLatch != null) {
				shutdownLatch.countDown();
			}
		}
	}

	private class CreatePartitionAndOffsetFunction implements Func2<Partition, Long, PartitionAndOffset> {
		@Override
		public PartitionAndOffset call(Partition partition, Long offset) {
			return new PartitionAndOffset(partition, offset);
		}
	}

	private class GetOffsetFunction implements Func1<PartitionAndOffset, Long> {
		@Override
		public Long call(PartitionAndOffset partitionAndOffset) {
			return partitionAndOffset.getOffset();
		}
	}

	private class ComputeMaximumOffsetByPartitionFunction implements Func1<GroupedObservable<Partition, PartitionAndOffset>, Observable<PartitionAndOffset>> {
		@Override
		public Observable<PartitionAndOffset> call(GroupedObservable<Partition, PartitionAndOffset> group) {
			return Observable.zip(Observable.just(group.getKey()),
					MathObservable.max(group.map(getOffsetFunction)),
					createPartitionAndOffsetFunction);
		}
	}

	private class GetPartitionFunction implements Func1<PartitionAndOffset, Partition> {
		@Override
		public Partition call(PartitionAndOffset partitionAndOffset) {
			return partitionAndOffset.getPartition();
		}
	}

	private class FindHighestOffsetsByPartitionFunction implements Func1<Observable<PartitionAndOffset>, Observable<PartitionAndOffset>> {
		@Override
		public Observable<PartitionAndOffset> call(Observable<PartitionAndOffset> windowBuffer) {
			return windowBuffer.groupBy(getPartition).flatMap(findHighestOffsetInPartitionGroup);
		}
	}
}
