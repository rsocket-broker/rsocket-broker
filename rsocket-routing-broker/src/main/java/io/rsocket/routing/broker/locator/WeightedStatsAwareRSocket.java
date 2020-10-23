package io.rsocket.routing.broker.locator;

import io.rsocket.RSocket;
import io.rsocket.loadbalance.WeightedStats;
import io.rsocket.util.RSocketProxy;

public class WeightedStatsAwareRSocket extends RSocketProxy {

	private final WeightedStats weightedStats;

	public WeightedStatsAwareRSocket(RSocket source, WeightedStats weightedStats) {
		super(source);
		this.weightedStats = weightedStats;
	}

	public WeightedStats weightedStats() {
		return weightedStats;
	}
}
