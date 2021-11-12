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

package io.rsocket.routing.broker.spring;

import io.rsocket.loadbalance.RoundRobinLoadbalanceStrategy;
import io.rsocket.loadbalance.WeightedLoadbalanceStrategy;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.acceptor.ClusterSocketAcceptor;
import io.rsocket.routing.broker.query.CombinedRSocketQuery;
import io.rsocket.routing.broker.rsocket.CompositeRSocketLocator;
import io.rsocket.routing.broker.rsocket.MulticastRSocketLocator;
import io.rsocket.routing.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.routing.broker.rsocket.UnicastRSocketLocator;
import io.rsocket.routing.broker.spring.cluster.BrokerConnections;
import io.rsocket.routing.broker.spring.cluster.ClusterController;
import io.rsocket.routing.broker.spring.cluster.ClusterJoinListener;
import io.rsocket.routing.broker.spring.cluster.ProxyConnections;
import io.rsocket.routing.broker.spring.cluster.RouteJoinListener;
import io.rsocket.routing.common.spring.DefaultClientTransportFactory;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.rsocket.RSocketMessagingAutoConfiguration;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.http.client.reactive.ReactorResourceFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerAutoConfigurationTests {

	@Test
	public void brokerDisabled() {
		new ReactiveWebApplicationContextRunner().withConfiguration(AutoConfigurations
				.of(BrokerAutoConfiguration.class, RSocketStrategiesAutoConfiguration.class, BrokerRSocketStrategiesAutoConfiguration.class))
				.withPropertyValues(BrokerProperties.PREFIX + ".enabled=false")
				.run(context -> assertThat(context).doesNotHaveBean(BrokerProperties.class));
	}

	@Test
	public void clusterDisabled() {
		new ReactiveWebApplicationContextRunner().withConfiguration(AutoConfigurations
				.of(BrokerAutoConfiguration.class, RSocketStrategiesAutoConfiguration.class, BrokerRSocketStrategiesAutoConfiguration.class))
				.withPropertyValues(BrokerProperties.Cluster.PREFIX+ ".enabled=false")
				.run(context -> {
					assertThat(context).hasSingleBean(BrokerProperties.class);
					assertThat(context).doesNotHaveBean(ClusterController.class);
				});
	}

	@Test
	public void brokerEnabled() {
		new ReactiveWebApplicationContextRunner().withConfiguration(AutoConfigurations
				.of(BrokerAutoConfiguration.class, RSocketStrategiesAutoConfiguration.class, BrokerRSocketStrategiesAutoConfiguration.class,
						RSocketMessagingAutoConfiguration.class))
				.run(context -> {
					assertThat(context).hasSingleBean(BrokerProperties.class);
					assertThat(context).hasSingleBean(RoundRobinLoadbalanceStrategy.class);
					assertThat(context).hasSingleBean(WeightedLoadbalanceStrategy.class);
					assertThat(context).hasBean(BrokerProperties.ROUND_ROBIN_LOAD_BALANCER_NAME);
					assertThat(context).hasBean(BrokerProperties.WEIGHTED_BALANCER_NAME);
					assertThat(context).hasSingleBean(BrokerProperties.class);
					assertThat(context).hasSingleBean(RSocketIndex.class);
					assertThat(context).hasSingleBean(RoutingTable.class);
					assertThat(context).hasSingleBean(ReactorResourceFactory.class);
					assertThat(context).hasSingleBean(CombinedRSocketQuery.class);
					assertThat(context).hasSingleBean(MulticastRSocketLocator.class);
					assertThat(context).hasSingleBean(UnicastRSocketLocator.class);
					assertThat(context).hasSingleBean(CompositeRSocketLocator.class);
					assertThat(context).hasSingleBean(AddressExtractor.class);
					assertThat(context).hasSingleBean(RoutingRSocketFactory.class);
					assertThat(context).hasSingleBean(RouteJoinListener.class);
					assertThat(context).hasSingleBean(MetadataExtractorBrokerSocketAcceptor.class);
					assertThat(context).hasSingleBean(DefaultServerTransportFactory.class);
					assertThat(context).hasBean("proxyRSocketServerBootstrap");
					assertThat(context).hasSingleBean(BrokerConnections.class);
					assertThat(context).hasSingleBean(ProxyConnections.class);
					// cluster configuration
					assertThat(context).hasSingleBean(ClusterController.class);
					assertThat(context).hasSingleBean(DefaultClientTransportFactory.class);
					assertThat(context).hasSingleBean(ClusterJoinListener.class);
					assertThat(context).hasSingleBean(ClusterSocketAcceptor.class);
					assertThat(context).hasBean("clusterRSocketServerBootstrap");
				});
	}
}
