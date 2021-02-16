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

package io.rsocket.routing.broker.rsocket;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import io.rsocket.RSocket;
import io.rsocket.loadbalance.LoadbalanceStrategy;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.query.RSocketQuery;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.RoutingType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UnicastRSocketLocatorTests {

	@Mock
	private RSocketQuery rSocketQuery;

	@Mock
	private RoutingTable routingTable;

	@Mock
	private LoadbalanceStrategy strategy;

	@Mock
	private LoadbalanceStrategy strategy2;

	private UnicastRSocketLocator locator;
	private Address address;

	@BeforeEach
	public void setup() {
		HashMap<String, LoadbalanceStrategy> loadbalancers = new HashMap<>();
		String defaultLoadBalancerName = "testlb";
		loadbalancers.put(defaultLoadBalancerName, strategy);
		loadbalancers.put("anotherlb", strategy2);

		locator = new UnicastRSocketLocator(rSocketQuery, routingTable, loadbalancers, defaultLoadBalancerName);
		address = Address.from(Id.random()).with(WellKnownKey.SERVICE_NAME, "service1")
				.build();
	}

	@Test
	public void supportWorks() {
		assertThat(locator.supports(RoutingType.UNICAST)).isTrue();
		assertThat(locator.supports(RoutingType.MULTICAST)).isFalse();
	}

	@Test
	public void emptyReturnsResolvingRSocketWorks() {
		when(routingTable.joinEvents(address.getTags())).thenReturn(Flux.empty());

		assertThat(locator.locate(address)).isInstanceOf(ResolvingRSocket.class);

		verifyNoInteractions(strategy, strategy2);
	}

	@Test
	public void oneReturnsRSocketWorks() {
		whenRSocketQuery(mock(RSocket.class));

		assertThat(locator.locate(address)).isNotInstanceOf(ResolvingRSocket.class).isInstanceOf(RSocket.class);

		verifyNoInteractions(strategy, strategy2);
	}

	@Test
	public void multipleDefaultLoadbalancerWorks() {
		whenRSocketQuery(mock(RSocket.class), mock(RSocket.class));

		when(strategy.select(anyList())).thenReturn(mock(RSocket.class));

		assertThat(locator.locate(address)).isNotInstanceOf(ResolvingRSocket.class).isInstanceOf(RSocket.class);

		verify(strategy).select(anyList());
		verifyNoInteractions(strategy2);
	}

	@Test
	public void multipleLoadbalancerHintWorks() {
		address = Address.from(Id.random()).with(WellKnownKey.SERVICE_NAME, "service1")
				.with(WellKnownKey.LB_METHOD, "anotherlb")
				.build();
		whenRSocketQuery(mock(RSocket.class), mock(RSocket.class));

		when(strategy2.select(anyList())).thenReturn(mock(RSocket.class));

		assertThat(locator.locate(address)).isNotInstanceOf(ResolvingRSocket.class).isInstanceOf(RSocket.class);

		verify(strategy2).select(anyList());
		verifyNoInteractions(strategy);
	}

	private OngoingStubbing<List<RSocket>> whenRSocketQuery(RSocket... rSocket) {
		return when(rSocketQuery.query(address.getTags())).thenReturn(Arrays.asList(rSocket));
	}

}
