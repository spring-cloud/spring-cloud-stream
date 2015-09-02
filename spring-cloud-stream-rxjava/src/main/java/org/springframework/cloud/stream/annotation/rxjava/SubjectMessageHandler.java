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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.handler.AbstractMessageProducingHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

/**
 * Adapts the item at a time delivery of a {@link org.springframework.messaging.MessageHandler}
 * by delegating processing to a {@link Observable}.
 * <p/>
 * The outputStream of the processor is used to create a message and send it to the output channel.  If the
 * input channel and output channel are connected to the {@link org.springframework.cloud.stream.binder.Binder},
 * then data delivered to the input stream via a call to onNext is invoked on the dispatcher thread of the binder
 * and sending a message to the output channel will involve IO operations on the binder.
 * <p/>
 * The implementation uses a SerializedSubject.  This has the advantage that the state of the Observabale
 * can be shared across all the incoming dispatcher threads that are invoking onNext.  It has the disadvantage
 * that processing and sending to the output channel will execute serially on one of the dispatcher threads.
 * <p/>
 * The use of this handler makes for a very natural first experience when processing data.  For example given
 * the stream <code></code>http | rxjava-processor | log</code> where the <code>rxjava-processor</code> does a
 * <code>buffer(5)</code> and then produces a single value.  Sending 10 messages to the http source will
 * result in 2 messages in the log, no matter how many dispatcher threads are used.
 * <p/>
 * You can modify what thread the outputStream subscriber, which does the send to the output channel,
 * will use by explicitly calling <code>observeOn</code> before returning the outputStream from your processor.
 * <p/>

 * All error handling is the responsibility of the processor implementation.
 *
 * @author Mark Pollack
 * @author Ilayaperumal Gopinathan
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SubjectMessageHandler extends AbstractMessageProducingHandler implements DisposableBean {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@SuppressWarnings("rawtypes")
	private final RxJavaProcessor processor;

	private final Subject subject;

	private final Subscription subscription;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public SubjectMessageHandler(RxJavaProcessor processor) {
		Assert.notNull(processor, "RxJava processor must not be null.");
		this.processor = processor;
		subject = new SerializedSubject(PublishSubject.create());
		Observable<?> outputStream = processor.process(subject);
		subscription = outputStream.subscribe(new Action1<Object>() {
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
				logger.error(throwable.getMessage(), throwable);
			}
		}, new Action0() {
			@Override
			public void call() {
				logger.error("Subscription close for [" + subscription + "]");
			}
		});
	}


	@Override
	//todo: support module input type
	protected void handleMessageInternal(Message<?> message) throws Exception {
		subject.onNext(message.getPayload());
	}

	@Override
	public void destroy() throws Exception {
		subscription.unsubscribe();
	}
}
