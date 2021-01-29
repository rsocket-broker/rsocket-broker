package io.rsocket.routing.broker.locator;

import io.rsocket.RSocket;
import io.rsocket.loadbalance.WeightedStats;
import io.rsocket.util.RSocketProxy;

public class WeightedStatsAwareRSocket extends RSocketProxy implements WeightedStats {

	private final WeightedStats weightedStats;

	public WeightedStatsAwareRSocket(RSocket source, WeightedStats weightedStats) {
		super(source);
		this.weightedStats = weightedStats;
	}

	@Override
	public double higherQuantileLatency() {
		return weightedStats.higherQuantileLatency();
	}

	@Override
	public double lowerQuantileLatency() {
		return weightedStats.lowerQuantileLatency();
	}

	@Override
	public int pending() {
		return weightedStats.pending();
	}

	@Override
	public double predictedLatency() {
		return weightedStats.predictedLatency();
	}

	@Override
	public double weightedAvailability() {
		return weightedStats.weightedAvailability();
	}

}
