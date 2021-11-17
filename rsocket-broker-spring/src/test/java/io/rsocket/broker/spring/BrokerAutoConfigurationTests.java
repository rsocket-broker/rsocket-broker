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

package io.rsocket.broker.spring;

import io.rsocket.broker.spring.AddressExtractor;
import io.rsocket.broker.spring.BrokerAutoConfiguration;
import io.rsocket.broker.spring.BrokerProperties;
import io.rsocket.broker.spring.BrokerRSocketStrategiesAutoConfiguration;
import io.rsocket.broker.spring.DefaultServerTransportFactory;
import io.rsocket.broker.spring.MetadataExtractorBrokerSocketAcceptor;
import io.rsocket.loadbalance.RoundRobinLoadbalanceStrategy;
import io.rsocket.loadbalance.WeightedLoadbalanceStrategy;
import io.rsocket.broker.RSocketIndex;
import io.rsocket.broker.RoutingTable;
import io.rsocket.broker.acceptor.ClusterSocketAcceptor;
import io.rsocket.broker.query.CombinedRSocketQuery;
import io.rsocket.broker.rsocket.CompositeRSocketLocator;
import io.rsocket.broker.rsocket.MulticastRSocketLocator;
import io.rsocket.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.broker.rsocket.UnicastRSocketLocator;
import io.rsocket.broker.spring.cluster.BrokerConnections;
import io.rsocket.broker.spring.cluster.ClusterController;
import io.rsocket.broker.spring.cluster.ClusterNodeConnectionManager;
import io.rsocket.broker.spring.cluster.ProxyConnections;
import io.rsocket.broker.spring.cluster.RouteJoinListener;
import io.rsocket.broker.common.spring.DefaultClientTransportFactory;
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
					assertThat(context).hasSingleBean(ClusterNodeConnectionManager.class);
					assertThat(context).hasSingleBean(ClusterSocketAcceptor.class);
					assertThat(context).hasBean("clusterRSocketServerBootstrap");
				});
	}
}
