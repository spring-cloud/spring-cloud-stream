/*
 * Copyright 2020-2020 the original author or authors.
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;

/**
 * This is Spring Cloud DynamoDB Adapter to be able to support {@code ListShards} operations.
 *
 * @author Asiel Caballero
 *
 * @since 2.0.3
 *
 * @see <a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html">ListShards</a>
 */
public class SpringDynamoDBAdapterClient extends AmazonDynamoDBStreamsAdapterClient {

	private static final String SEPARATOR = "!!##%%";

	public SpringDynamoDBAdapterClient(AmazonDynamoDBStreams amazonDynamoDBStreams) {
		super(amazonDynamoDBStreams);
	}

	/**
	 * List shards for a DynamoDB Stream using its {@code DescribeStream} API, as they don't support
	 * {@code ListShards} operations. Returns the result adapted to use the AmazonKinesis model.
	 * @param request Container for the necessary parameters to execute the ListShards service method
	 * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model
	 */
	@Override
	public ListShardsResult listShards(ListShardsRequest request) {
		try {
			if (request.getNextToken() != null && request.getStreamName() != null) {
				throw new InvalidArgumentException("NextToken and StreamName cannot be provided together.");
			}

			String streamName = request.getStreamName();
			String exclusiveStartShardId = request.getExclusiveStartShardId();

			if (request.getNextToken() != null) {
				String[] split = request.getNextToken().split(SEPARATOR);

				if (split.length != 2) {
					throw new InvalidArgumentException("Invalid ShardIterator");
				}

				streamName = split[0];
				exclusiveStartShardId = split[1];
			}

			DescribeStreamRequest dsr = new DescribeStreamRequest()
					.withStreamName(streamName)
					.withExclusiveStartShardId(exclusiveStartShardId)
					.withLimit(request.getMaxResults());
			StreamDescription streamDescription = describeStream(dsr).getStreamDescription();

			ListShardsResult result = new ListShardsResult()
					.withShards(streamDescription.getShards());

			if (streamDescription.getHasMoreShards()) {
				result.withNextToken(buildFakeNextToken(streamName,
						streamDescription.getShards().get(streamDescription.getShards().size() - 1).getShardId()));
			}

			return result;
		}
		catch (AmazonDynamoDBException ex) {
			ResourceNotFoundException resourceEx = new ResourceNotFoundException(ex.getMessage());
			resourceEx.setStackTrace(ex.getStackTrace());
			throw resourceEx;
		}
	}

	private String buildFakeNextToken(String streamName, String lastShard) {
		return lastShard != null ? streamName + SEPARATOR + lastShard : null;
	}

}
