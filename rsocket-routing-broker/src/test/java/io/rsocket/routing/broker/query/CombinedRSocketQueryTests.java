/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.routing.broker.query;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import io.rsocket.RSocket;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.RouteJoin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CombinedRSocketQueryTests {

	private CombinedRSocketQuery query;

	@Mock
	private RoutingTable routingTable;

	@Mock
	private RSocketIndex rSocketIndex;

	@Mock
	private Function<BrokerInfo, RSocket> rSocketMapper;

	@BeforeEach
	public void setup() {
		query = new CombinedRSocketQuery(Id.random(), routingTable, rSocketIndex, rSocketMapper);
	}

	@Test
	public void emptyQueryWorks() {
		List<RSocket> rSockets = query.query(getTags());

		assertThat(rSockets).isEmpty();
	}

	@Test
	public void emptyTagsThrowsException() {
		assertThatThrownBy(() -> query.query(Tags.builder().buildTags())).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> query.query(null)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void queryLocalRSocketWorks() {
		Tags tags = getTags();
		whenRSocketIndex(tags);

		List<RSocket> rSockets = query.query(tags);

		assertThat(rSockets).singleElement().isInstanceOf(RSocket.class);
	}

	@Test
	public void queryRemoteRSocketWorks() {
		Tags tags = getTags();
		whenRoutingTable(tags);

		List<RSocket> rSockets = query.query(tags);

		assertThat(rSockets).singleElement().isNotNull();
	}

	@Test
	public void queryLocalAndRemoteRemoteRSocketWorks() {
		Tags tags = getTags();
		whenRoutingTable(tags);

		whenRSocketIndex(tags);

		List<RSocket> rSockets = query.query(tags);

		assertThat(rSockets).hasSize(2).doesNotContainNull();
	}

	private void whenRoutingTable(Tags tags) {
		RouteJoin routeJoin = RouteJoin.builder().brokerId(Id.random()).routeId(Id.random()).serviceName("service1").build();
		when(routingTable.find(tags)).thenReturn(Collections.singletonList(routeJoin));
		when(rSocketMapper.apply(any(BrokerInfo.class))).thenReturn(mock(RSocket.class));
	}


	private void whenRSocketIndex(Tags tags) {
		when(rSocketIndex.query(tags)).thenReturn(Collections.singletonList(mock(RSocket.class)));
	}

	private Tags getTags() {
		return Tags.builder().with(WellKnownKey.SERVICE_NAME, "service1").buildTags();
	}
}
