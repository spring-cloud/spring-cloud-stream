/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis.adapter;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.SequenceNumberRange;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Asiel Caballero
 *
 * @since 2.0.3
 */
public class SpringDynamoDBAdapterClientTests {

	private final AmazonDynamoDBStreams amazonDynamoDBStreams = new InMemoryAmazonDynamoDBStreams();

	private final SpringDynamoDBAdapterClient springDynamoDBAdapterClient =
			new SpringDynamoDBAdapterClient(amazonDynamoDBStreams);

	@Test
	public void listShardsForNotFoundStream() {
		assertThatExceptionOfType(ResourceNotFoundException.class)
				.isThrownBy(() -> springDynamoDBAdapterClient.listShards(new ListShardsRequest()
						.withStreamName("not-found")));
	}

	@Test
	public void listShardsWithInvalidToken() {
		assertThatExceptionOfType(InvalidArgumentException.class)
				.isThrownBy(() -> springDynamoDBAdapterClient.listShards(new ListShardsRequest()
						.withNextToken("invalid-token")));
	}

	@Test
	public void listShardsWithTokenAndStreamName() {
		assertThatExceptionOfType(InvalidArgumentException.class)
				.isThrownBy(() ->
						springDynamoDBAdapterClient.listShards(new ListShardsRequest()
								.withStreamName(InMemoryAmazonDynamoDBStreams.STREAM_ARN)
								.withNextToken("valid!!##%%token")));
	}

	@Test
	public void listShardsNoPagination() {
		ListShardsResult listShards = springDynamoDBAdapterClient.listShards(new ListShardsRequest()
				.withStreamName(InMemoryAmazonDynamoDBStreams.STREAM_ARN));

		assertThat(listShards.getNextToken()).isNull();
		assertThat(listShards.getShards().size()).isEqualTo(InMemoryAmazonDynamoDBStreams.SHARDS.size());
	}

	@Test
	public void listShardWithTokenPagination() {
		int maxResults = (InMemoryAmazonDynamoDBStreams.SHARDS.size() / 2)
				+ (InMemoryAmazonDynamoDBStreams.SHARDS.size() % 2);
		ListShardsResult listShards = springDynamoDBAdapterClient.listShards(new ListShardsRequest()
				.withStreamName(InMemoryAmazonDynamoDBStreams.STREAM_ARN)
				.withMaxResults(maxResults));

		assertThat(listShards.getNextToken()).isNotNull();
		assertThat(listShards.getShards().size()).isEqualTo(maxResults);

		listShards = springDynamoDBAdapterClient.listShards(new ListShardsRequest()
				.withNextToken(listShards.getNextToken()));

		assertThat(listShards.getNextToken()).isNull();
		assertThat(listShards.getShards().size()).isEqualTo(InMemoryAmazonDynamoDBStreams.SHARDS.size() - maxResults);
	}

	@Test
	public void listShardsWithShardIdPagination() {
		ListShardsResult listShards = springDynamoDBAdapterClient.listShards(new ListShardsRequest()
				.withStreamName(InMemoryAmazonDynamoDBStreams.STREAM_ARN)
				.withExclusiveStartShardId(InMemoryAmazonDynamoDBStreams.SHARDS.get(2).getShardId())
				.withMaxResults(1));

		assertThat(listShards.getNextToken()).isNotNull();
		assertThat(listShards.getShards().size()).isEqualTo(1);
		assertThat(listShards.getShards().get(0).getShardId())
				.isEqualTo(InMemoryAmazonDynamoDBStreams.SHARDS.get(3).getShardId());
	}


	private static class InMemoryAmazonDynamoDBStreams extends AbstractAmazonDynamoDBStreams {

		private static final String TABLE_NAME = "test-streams";

		private static final String STREAM_LABEL = "2020-10-21T11:49:13.355";

		private static final String STREAM_ARN = String
				.format("arn:aws:dynamodb:%s:%s:table/%s/stream/%s",
					Regions.DEFAULT_REGION.getName(), "000000000000", TABLE_NAME, STREAM_LABEL);

		private static final List<Shard> SHARDS =
				Arrays.asList(
						buildShard("shardId-00000001603195033866-c5d0c2b1", "51100000000002515059163", "51300000000002521847055"),
						buildShard("shardId-00000001603208699318-b15c42af", "804300000000026046960744", "804300000000026046960744")
								.withParentShardId("shardId-00000001603195033866-c5d0c2b1"),
						buildShard("shardId-00000001603223404428-90b80e6c", "1613900000000033324335703", "1613900000000033324335703")
								.withParentShardId("shardId-00000001603208699318-b15c42af"),
						buildShard("shardId-00000001603237029376-bd9c40dd", "2364600000000001701561758", "2364600000000001701561758")
								.withParentShardId("shardId-00000001603223404428-90b80e6c"),
						buildShard("shardId-00000001603249855034-b917a47f", "3071400000000035046301998", "3071400000000035046301998")
								.withParentShardId("shardId-00000001603237029376-bd9c40dd"));

		private final StreamDescription streamDescription = new StreamDescription()
				.withStreamArn(STREAM_ARN)
				.withStreamLabel(STREAM_LABEL)
				.withStreamViewType(StreamViewType.KEYS_ONLY)
				.withCreationRequestDateTime(new Date())
				.withTableName(TABLE_NAME)
				.withKeySchema(new KeySchemaElement("name", KeyType.HASH))
				.withStreamStatus(StreamStatus.ENABLED);

		private static Shard buildShard(String parentShardIdString, String startingSequenceNumber,
				String endingSequenceNumber) {

			return new Shard()
					.withShardId(parentShardIdString)
					.withSequenceNumberRange(new SequenceNumberRange()
							.withStartingSequenceNumber(startingSequenceNumber)
							.withEndingSequenceNumber(endingSequenceNumber));
		}

		@Override
		public DescribeStreamResult describeStream(DescribeStreamRequest request) {
			// Invalid StreamArn (Service: AmazonDynamoDBStreams; Status Code: 400;
			// Error Code: ValidationException; Request ID: <Request ID>; Proxy: null)
			if (!STREAM_ARN.equals(request.getStreamArn())) {
				throw new AmazonDynamoDBException("Invalid StreamArn");
			}

			int limit = request.getLimit() != null ? request.getLimit() : 100;

			int shardIndex = request.getExclusiveStartShardId() != null
					? findShardIndex(request.getExclusiveStartShardId())
					: 0;
			int lastShardIndex = Math.min(shardIndex + limit, SHARDS.size());
			streamDescription.setShards(SHARDS.subList(shardIndex, lastShardIndex));
			streamDescription.setLastEvaluatedShardId(
					lastShardIndex != SHARDS.size() ? SHARDS.get(lastShardIndex).getShardId() : null);

			return new DescribeStreamResult()
					.withStreamDescription(streamDescription);
		}

		private int findShardIndex(String shardId) {
			int i = 0;
			// checkstyle forced this way of writing it
			while (i < SHARDS.size() && !SHARDS.get(i).getShardId().equals(shardId)) {
				i++;
			}

			if (i + 1 >= SHARDS.size()) {
				throw new RuntimeException("ShardId not found");
			}

			return i + 1;
		}

	}

}
