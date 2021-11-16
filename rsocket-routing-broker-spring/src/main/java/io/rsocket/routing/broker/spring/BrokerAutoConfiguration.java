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

package io.rsocket.routing.broker.spring;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import io.rsocket.SocketAcceptor;
import io.rsocket.loadbalance.LoadbalanceStrategy;
import io.rsocket.loadbalance.RoundRobinLoadbalanceStrategy;
import io.rsocket.loadbalance.WeightedLoadbalanceStrategy;
import io.rsocket.loadbalance.WeightedStats;
import io.rsocket.loadbalance.WeightedStatsRequestInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.routing.broker.Broker;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.routing.broker.acceptor.ClusterSocketAcceptor;
import io.rsocket.routing.broker.query.CombinedRSocketQuery;
import io.rsocket.routing.broker.query.RSocketQuery;
import io.rsocket.routing.broker.rsocket.CompositeRSocketLocator;
import io.rsocket.routing.broker.rsocket.MulticastRSocketLocator;
import io.rsocket.routing.broker.rsocket.RSocketLocator;
import io.rsocket.routing.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.routing.broker.rsocket.UnicastRSocketLocator;
import io.rsocket.routing.broker.rsocket.WeightedStatsAwareRSocket;
import io.rsocket.routing.broker.spring.cluster.BrokerConnections;
import io.rsocket.routing.broker.spring.cluster.ClusterController;
import io.rsocket.routing.broker.spring.cluster.ClusterJoinListener;
import io.rsocket.routing.broker.spring.cluster.ClusterMonitor;
import io.rsocket.routing.broker.spring.cluster.MessageHandlerClusterSocketAcceptor;
import io.rsocket.routing.broker.spring.cluster.ProxyConnections;
import io.rsocket.routing.broker.spring.cluster.RouteJoinListener;
import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.common.spring.DefaultClientTransportFactory;
import io.rsocket.routing.common.spring.MimeTypes;
import io.rsocket.routing.frames.RoutingFrame;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.rsocket.context.RSocketServerBootstrap;
import org.springframework.boot.rsocket.netty.NettyRSocketServer;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.boot.rsocket.server.RSocketServerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.reactive.ReactorResourceFactory;
import org.springframework.messaging.rsocket.DefaultMetadataExtractor;
import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableConfigurationProperties
@ConditionalOnProperty(name = BrokerProperties.PREFIX + ".enabled", matchIfMissing = true)
@AutoConfigureAfter({RSocketStrategiesAutoConfiguration.class, BrokerRSocketStrategiesAutoConfiguration.class})
public class BrokerAutoConfiguration implements InitializingBean {

	private static final Log log = LogFactory.getLog(BrokerAutoConfiguration.class);

	private final ApplicationContext context;

	public BrokerAutoConfiguration(ApplicationContext context) {
		this.context = context;
	}

	@Override
	public void afterPropertiesSet() {
		Hooks.onErrorDropped(t -> {
			if (t instanceof CancellationException && log.isDebugEnabled()) {
				log.debug("dropped cancellation error", t);
			}
			else if (log.isWarnEnabled()) {
				log.warn("dropped error", t);
			}
		});

		RSocketStrategies rSocketStrategies = this.context.getBean(RSocketStrategies.class);
		MetadataExtractor metadataExtractor = rSocketStrategies.metadataExtractor();

		if (metadataExtractor instanceof DefaultMetadataExtractor) {
			DefaultMetadataExtractor extractor = (DefaultMetadataExtractor) metadataExtractor;
			// adds all RoutingFrame impls such as RouteJoin, RouteSetup, etc..
			// to the spring encoding/decoding framework.
			extractor.metadataToExtract(MimeTypes.ROUTING_FRAME_MIME_TYPE, RoutingFrame.class,
					MimeTypes.ROUTING_FRAME_METADATA_KEY);
		}
	}

	@Bean
	public RSocketServerCustomizer weightedStatsCustomizer() {
		return rSocketServer -> rSocketServer.interceptors(ir -> ir.forRequestsInResponder(rSocket -> {
			final WeightedStatsRequestInterceptor weightedStatsRequestInterceptor =
					new WeightedStatsRequestInterceptor();
			ir.forRequester((RSocketInterceptor) rSocket1 -> new WeightedStatsAwareRSocket(rSocket1, weightedStatsRequestInterceptor));
			return weightedStatsRequestInterceptor;
		}));
	}

	@Bean
	// TODO: Broker class needed?
	public Broker broker() {
		return new Broker();
	}

	@Bean
	public BrokerProperties brokerProperties() {
		return new BrokerProperties();
	}

	@Bean
	public RSocketIndex rSocketIndex() {
		return new RSocketIndex();
	}

	@Bean
	public RoutingTable routingTable() {
		return new RoutingTable();
	}

	@Bean
	@ConditionalOnMissingBean
	public ReactorResourceFactory reactorResourceFactory() {
		return new ReactorResourceFactory();
	}

	@Bean(name = BrokerProperties.WEIGHTED_BALANCER_NAME)
	public WeightedLoadbalanceStrategy weightedLoadbalanceStrategy() {
		return WeightedLoadbalanceStrategy.builder()
				.weightedStatsResolver(rSocket -> ((WeightedStats) rSocket))
				.build();
	}

	@Bean(name = BrokerProperties.ROUND_ROBIN_LOAD_BALANCER_NAME)
	public RoundRobinLoadbalanceStrategy roundRobinLoadbalanceStrategy() {
		return new RoundRobinLoadbalanceStrategy();
	}

	@Bean
	public ProxyConnections proxyConnections() {
		return new ProxyConnections();
	}

	@Bean
	public BrokerConnections brokerConnections() {
		return new BrokerConnections();
	}

	@Bean
	public CombinedRSocketQuery combinedRSocketQuery(BrokerProperties properties,
			RoutingTable routingTable, RSocketIndex index, ProxyConnections connections) {
		return new CombinedRSocketQuery(properties.getBrokerId(), routingTable, index, connections::get);
	}

	@Bean
	public MulticastRSocketLocator multicastRSocketLocator(RSocketQuery rSocketQuery) {
		return new MulticastRSocketLocator(rSocketQuery);
	}

	@Bean
	public UnicastRSocketLocator unicastRSocketLocator(RSocketQuery rSocketQuery, RoutingTable routingTable,
			Map<String, LoadbalanceStrategy> loadbalancers, BrokerProperties properties) {
		return new UnicastRSocketLocator(rSocketQuery, routingTable, loadbalancers, properties.getDefaultLoadBalancer());
	}

	@Bean
	@Primary
	public CompositeRSocketLocator compositeRSocketLocator(ObjectProvider<RSocketLocator> locators) {
		return new CompositeRSocketLocator(locators.orderedStream().collect(Collectors.toList()));
	}

	@Bean
	public AddressExtractor addressTagsExtractor(RSocketStrategies strategies) {
		return new AddressExtractor(strategies.metadataExtractor());
	}

	@Bean
	public RoutingRSocketFactory routingRSocketFactory(RSocketLocator locator,
			AddressExtractor tagsExtractor) {
		return new RoutingRSocketFactory(locator, tagsExtractor);
	}

	@Bean
	public RouteJoinListener routeJoinListener(BrokerProperties properties,
			RoutingTable routingTable, BrokerConnections brokerConnections) {
		return new RouteJoinListener(properties, routingTable, brokerConnections);
	}

	@Bean
	public MetadataExtractorBrokerSocketAcceptor metadataExtractorBrokerSocketAcceptor(
			RSocketStrategies rSocketStrategies, RoutingTable routingTable,
			RSocketIndex rSocketIndex, RoutingRSocketFactory routingRSocketFactory,
			BrokerProperties properties, ProxyConnections proxyConnections) {
		return new MetadataExtractorBrokerSocketAcceptor(properties, routingTable, rSocketIndex,
				routingRSocketFactory, rSocketStrategies
				.metadataExtractor(), proxyConnections);
	}

	private static String findTransportName(URI uri) {
		return uri.getScheme();
	}

	@Configuration
	@ConditionalOnProperty(name = BrokerProperties.Cluster.PREFIX + ".enabled", matchIfMissing = true)
	protected static class ClusterConfiguration {

		@Bean
		public ClusterController clusterController(BrokerProperties properties,
				BrokerConnections brokerConnections, RoutingTable routingTable) {
			return new ClusterController(properties, brokerConnections, routingTable);
		}

		@Bean
		@ConditionalOnMissingBean
		public DefaultClientTransportFactory defaultClientTransportFactory() {
			return new DefaultClientTransportFactory();
		}

		@Bean
		public ClusterJoinListener clusterJoinListener(BrokerProperties properties,
				BrokerConnections brokerConnections, ProxyConnections proxyConnections,
				RSocketMessageHandler messageHandler, RSocketStrategies strategies,
				ObjectProvider<ClientTransportFactory> transportFactories,
				RoutingRSocketFactory routingRSocketFactory) {
			return new ClusterJoinListener(properties, brokerConnections, proxyConnections,
					messageHandler, strategies, routingRSocketFactory, transportFactories);
		}

		@Bean
		public ClusterSocketAcceptor clusterSocketAcceptor(RSocketMessageHandler messageHandler) {
			return new MessageHandlerClusterSocketAcceptor(messageHandler);
		}

		@Bean
		public BrokerRSocketServerBootstrap clusterRSocketServerBootstrap(
				BrokerProperties properties,
				ObjectProvider<ServerTransportFactory> transportFactories,
				ClusterSocketAcceptor clusterSocketAcceptor) {
			RSocketServerFactory serverFactory = findRSocketServerFactory(properties.getCluster().getUri(), transportFactories);
			return new BrokerRSocketServerBootstrap("cluster", findTransportName(properties.getCluster().getUri()), serverFactory, clusterSocketAcceptor);
		}

	}

	@Configuration
	//TODO: also conditional on cluster enabled
	@ConditionalOnProperty(BrokerProperties.Cluster.PREFIX + ".monitor.enabled")
	@EnableScheduling
	protected static class ClusterMonitorConfiguration {

		@Bean
		public ClusterMonitor clusterMonitor(BrokerConnections brokerConnections, ProxyConnections proxyConnections) {
			return new ClusterMonitor(brokerConnections, proxyConnections);
		}

		@Bean
		public RoutingTableMonitor routingTableMonitor(RoutingTable routingTable) {
			return new RoutingTableMonitor(routingTable);
		}

	}

	@Bean
	public DefaultServerTransportFactory defaultServerTransportFactory(ReactorResourceFactory resourceFactory,
			ObjectProvider<RSocketServerCustomizer> processors) {
		return new DefaultServerTransportFactory(resourceFactory, processors);
	}

	@Bean
	public BrokerRSocketServerBootstrap proxyRSocketServerBootstrap(
			BrokerProperties properties,
			ObjectProvider<ServerTransportFactory> transportFactories,
			BrokerSocketAcceptor brokerSocketAcceptor) {
		RSocketServerFactory serverFactory = findRSocketServerFactory(properties.getUri(), transportFactories);
		return new BrokerRSocketServerBootstrap("broker", findTransportName(properties.getUri()), serverFactory, brokerSocketAcceptor);
	}

	private static RSocketServerFactory findRSocketServerFactory(URI uri, ObjectProvider<ServerTransportFactory> factories) {
		return factories.orderedStream().filter(factory -> factory.supports(uri)).findFirst()
				.map(factory -> factory.create(uri))
				.orElseThrow(() -> new IllegalArgumentException("Unknown transport " + uri));
	}

	/* for testing */ static class BrokerRSocketServerBootstrap extends RSocketServerBootstrap {

		// purposefully using NettyRSocketServer
		private static final Logger logger = LoggerFactory.getLogger(NettyRSocketServer.class);
		private final String type;
		private final String transport;
		private final RSocketServerFactory serverFactory;

		public BrokerRSocketServerBootstrap(String type, String transport,
				RSocketServerFactory serverFactory, SocketAcceptor socketAcceptor) {
			super(serverFactory, socketAcceptor);
			this.type = type;
			this.transport = transport;
			this.serverFactory = serverFactory;
		}

		@Override
		public void start() {
			logger.info("Netty RSocket starting {} with {}", type, transport);
			super.start();
		}

		/* for testing */ RSocketServerFactory getServerFactory() {
			return this.serverFactory;
		}
	}

}
