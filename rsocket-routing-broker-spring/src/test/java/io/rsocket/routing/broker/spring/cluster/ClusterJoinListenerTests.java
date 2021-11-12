/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.routing.broker.spring.cluster;

import java.net.URI;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.core.DefaultConnectionSetupPayload;
import io.rsocket.routing.broker.spring.BrokerProperties;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.BrokerInfoFlyweight;
import io.rsocket.routing.frames.FrameHeaderFlyweight;
import io.rsocket.routing.frames.FrameType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterJoinListenerTests {

	@Test
	void testGetConnectionSetupPayload() {
		Id id = Id.from("00000000-0000-0000-0000-000000000011");
		BrokerProperties properties = new BrokerProperties();
		properties.setBrokerId(id);
		properties.setUri(URI.create("tcp://localhost:1111"));
		properties.getCluster().setUri(URI.create("tcp://localhost:2222"));
		DefaultConnectionSetupPayload setupPayload = ClusterJoinListener
				.getConnectionSetupPayload(ByteBufAllocator.DEFAULT, properties);
		ByteBuf data = setupPayload.data();
		assertThat(FrameHeaderFlyweight.frameType(data)).isEqualTo(FrameType.BROKER_INFO);
		assertThat(BrokerInfoFlyweight.brokerId(data)).isEqualTo(id);
		assertThat(BrokerInfoFlyweight.timestamp(data)).isGreaterThan(0);
		Tags tags = BrokerInfoFlyweight.tags(data);
		assertThat(tags).isNotNull();
		assertThat(tags.asMap()).containsEntry(WellKnownKey.BROKER_PROXY_URI.getKey(), properties.getUri().toString())
				.containsEntry(WellKnownKey.BROKER_CLUSTER_URI.getKey(), properties.getCluster().getUri().toString());
	}

}
