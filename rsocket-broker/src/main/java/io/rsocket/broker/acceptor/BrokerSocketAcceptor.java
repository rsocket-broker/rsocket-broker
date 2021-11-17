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

package io.rsocket.broker.acceptor;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.broker.RSocketIndex;
import io.rsocket.broker.RoutingTable;
import io.rsocket.broker.rsocket.ErrorOnDisconnectRSocket;
import io.rsocket.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.broker.common.Id;
import io.rsocket.broker.common.WellKnownKey;
import io.rsocket.broker.frames.BrokerInfo;
import io.rsocket.broker.frames.RouteJoin;
import io.rsocket.broker.frames.RouteSetup;
import io.rsocket.broker.frames.BrokerFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * SocketAcceptor for routable connections either from clients or other brokers.
 */
public class BrokerSocketAcceptor implements SocketAcceptor {
	protected static final Logger logger = LoggerFactory
			.getLogger(BrokerSocketAcceptor.class);

	protected final Id brokerId;
	protected final RoutingTable routingTable;
	protected final RSocketIndex rSocketIndex;
	protected final Function<ConnectionSetupPayload, BrokerFrame> payloadExtractor;
	protected final BiConsumer<BrokerInfo, RSocket> brokerInfoConsumer;
	protected final Consumer<BrokerInfo> brokerInfoCleaner;
	protected final RoutingRSocketFactory routingRSocketFactory;

	public BrokerSocketAcceptor(Id brokerId, RoutingTable routingTable,
			RSocketIndex rSocketIndex, RoutingRSocketFactory routingRSocketFactory,
			Function<ConnectionSetupPayload, BrokerFrame> payloadExtractor,
			BiConsumer<BrokerInfo, RSocket> brokerInfoConsumer,
			Consumer<BrokerInfo> brokerInfoCleaner) {
		this.brokerId = brokerId;
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
		this.routingRSocketFactory = routingRSocketFactory;
		this.payloadExtractor = payloadExtractor;
		this.brokerInfoConsumer = brokerInfoConsumer;
		this.brokerInfoCleaner = brokerInfoCleaner;

		logger.info("Starting Broker {}", brokerId);
	}

	@Override
	public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {

		try {
			BrokerFrame BrokerFrame = payloadExtractor.apply(setup);
			Runnable doCleanup = () -> cleanup(BrokerFrame);

			logger.debug("accept {}", BrokerFrame);

			RSocket wrapSendingSocket = wrapSendingSocket(sendingSocket, BrokerFrame);
			if (BrokerFrame instanceof BrokerInfo) {
				// this is another broker connecting

				brokerInfoConsumer.accept((BrokerInfo) BrokerFrame, wrapSendingSocket);

				return finalize(sendingSocket, doCleanup);
			} else if (BrokerFrame instanceof RouteSetup) {
				RouteSetup routeSetup = (RouteSetup) BrokerFrame;
				// TODO: metrics
				// TODO: error on disconnect?
				return Mono.defer(() -> {
					// TODO: deal with existing connection for routeSetup.routeId

					// create RouteJoin before indexing or RoutingTable
					RouteJoin routeJoin = toRouteJoin(routeSetup);

					// update index before RoutingTable
					// adds sendingSocket to rSocketIndex for later lookup
					rSocketIndex.put(routeJoin.getRouteId(), wrapSendingSocket,
							routeJoin.getTags());

					// update routing table with incoming route.
					routingTable.add(routeJoin);

					return finalize(sendingSocket, doCleanup);
				});
			}

			throw new IllegalStateException("RouteSetup not found in metadata");
		}
		catch (Exception e) {
			logger.error("Error accepting setup", e);
//			doCleanup.run();
			return Mono.error(e);
		}
	}

	private Mono<RSocket> finalize(RSocket sendingSocket, Runnable doCleanup) {
		RSocket receivingSocket = routingRSocketFactory.create();
		Flux.first(receivingSocket.onClose(), sendingSocket.onClose())
				.doFinally(s -> doCleanup.run())
				.subscribe();
		return Mono.just(receivingSocket);
	}

	private void cleanup(BrokerFrame BrokerFrame) {
		if (BrokerFrame instanceof BrokerInfo) {
			BrokerInfo brokerInfo = (BrokerInfo) BrokerFrame;
			brokerInfoCleaner.accept(brokerInfo);
		}
		else if (BrokerFrame instanceof RouteSetup) {
			RouteSetup routeSetup = (RouteSetup) BrokerFrame;
			Id routeId = routeSetup.getRouteId();
			routingTable.remove(routeId);
			rSocketIndex.remove(routeId);
		}
	}

	private RSocket wrapSendingSocket(RSocket sendingSocket, BrokerFrame BrokerFrame) {
		ErrorOnDisconnectRSocket rSocket = new ErrorOnDisconnectRSocket(sendingSocket);
		rSocket.onClose().doFinally(s -> logger.info("Closing socket for {}", BrokerFrame));
		return rSocket;
	}

	/**
	 * Creates RouteJoin with current brokerId and enriches tags with routeId and serviceName.
 	 */
	private RouteJoin toRouteJoin(RouteSetup routeSetup) {
		return RouteJoin.builder()
				.brokerId(brokerId)
				.routeId(routeSetup.getRouteId())
				.serviceName(routeSetup.getServiceName())
				.with(routeSetup.getTags())
				.with(WellKnownKey.ROUTE_ID, routeSetup.getRouteId().toString())
				.with(WellKnownKey.SERVICE_NAME, routeSetup.getServiceName())
				.build();
	}
}
