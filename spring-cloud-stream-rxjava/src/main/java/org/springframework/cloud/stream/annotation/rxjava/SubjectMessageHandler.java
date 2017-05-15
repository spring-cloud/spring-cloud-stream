/*
 * Copyright 2015-2016 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.handler.AbstractMessageProducingHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Adapts the item at a time delivery of a
 * {@link org.springframework.messaging.MessageHandler} by delegating processing to a
 * {@link Observable}.
 * <p/>
 * The outputStream of the processor is used to create a message and send it to the output
 * channel. If the input channel and output channel are connected to the
 * {@link org.springframework.cloud.stream.binder.Binder}, then data delivered to the
 * input stream via a call to onNext is invoked on the dispatcher thread of the binder and
 * sending a message to the output channel will involve IO operations on the binder.
 * <p/>
 * The implementation uses a SerializedSubject. This has the advantage that the state of
 * the Observabale can be shared across all the incoming dispatcher threads that are
 * invoking onNext. It has the disadvantage that processing and sending to the output
 * channel will execute serially on one of the dispatcher threads.
 * <p/>
 * The use of this handler makes for a very natural first experience when processing data.
 * For example given the stream <code></code>http | rxjava-processor | log</code> where
 * the <code>rxjava-processor</code> does a <code>buffer(5)</code> and then produces a
 * single value. Sending 10 messages to the http source will result in 2 messages in the
 * log, no matter how many dispatcher threads are used.
 * <p/>
 * You can modify what thread the outputStream subscriber, which does the send to the
 * output channel, will use by explicitly calling <code>observeOn</code> before returning
 * the outputStream from your processor.
 * <p/>
 * 
 * All error handling is the responsibility of the processor implementation.
 *
 * @author Mark Pollack
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
@Deprecated
public class SubjectMessageHandler extends AbstractMessageProducingHandler implements SmartLifecycle {

	private final Log logger = LogFactory.getLog(getClass());

	@SuppressWarnings("rawtypes")
	private final RxJavaProcessor processor;

	private volatile Subject subject;

	private volatile Subscription subscription;

	private volatile boolean running;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public SubjectMessageHandler(RxJavaProcessor processor) {
		Assert.notNull(processor, "RxJava processor must not be null.");
		this.processor = processor;
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			this.subject = new SerializedSubject(PublishSubject.create());
			Observable<?> outputStream = this.processor.process(this.subject);
			this.subscription = outputStream.subscribe(new Action1<Object>() {

				@Override
				public void call(Object outputObject) {
					if (ClassUtils.isAssignable(Message.class, outputObject.getClass())) {
						getOutputChannel().send((Message) outputObject);
					}
					else {
						getOutputChannel().send(MessageBuilder.withPayload(outputObject).build());
					}
				}
			}, new Action1<Throwable>() {

				@Override
				public void call(Throwable throwable) {
					SubjectMessageHandler.this.logger.error(throwable.getMessage(), throwable);
				}
			}, new Action0() {

				@Override
				public void call() {
					SubjectMessageHandler.this.logger
							.info("Subscription close for [" + SubjectMessageHandler.this.subscription + "]");
				}
			});
			this.running = true;
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public void stop(Runnable callback) {
		if (this.running) {
			stop();
			if (callback != null) {
				callback.run();
			}
		}
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		this.subject.onNext(message.getPayload());
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.subject.onCompleted();
			this.subscription.unsubscribe();
			this.subscription = null;
			this.subject = null;
			this.running = false;
		}
	}
}
