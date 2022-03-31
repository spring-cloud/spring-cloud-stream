/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * @author Artem Bilan
 *
 * @since 2.0
 */
public class KinesisBinderHealthIndicator implements HealthIndicator {

	private final KinesisMessageChannelBinder kinesisMessageChannelBinder;

	public KinesisBinderHealthIndicator(KinesisMessageChannelBinder kinesisMessageChannelBinder) {
		this.kinesisMessageChannelBinder = kinesisMessageChannelBinder;
	}

	@Override
	public Health health() {
		AmazonKinesisAsync amazonKinesis = this.kinesisMessageChannelBinder.getAmazonKinesis();
		List<String> streamsInUse = new ArrayList<>(this.kinesisMessageChannelBinder.getStreamsInUse());
		for (String stream : streamsInUse) {
			while (true) {
				try {
					amazonKinesis.listShards(new ListShardsRequest().withStreamName(stream).withMaxResults(1));
					break;
				}
				catch (LimitExceededException ex) {
					try {
						TimeUnit.SECONDS.sleep(1);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return Health.down(ex).build();
					}
				}
				catch (Exception ex) {
					return Health.down(ex).build();
				}
			}
		}
		return Health.up().build();
	}

}
