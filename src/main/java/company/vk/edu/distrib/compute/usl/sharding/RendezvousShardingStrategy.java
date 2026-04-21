package company.vk.edu.distrib.compute.usl.sharding;

import java.util.List;
import java.util.Objects;

public final class RendezvousShardingStrategy implements ShardingStrategy {
    private final List<String> endpoints;

    public RendezvousShardingStrategy(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String resolveOwner(String key) {
        Objects.requireNonNull(key, "key");
        String selectedEndpoint = null;
        long selectedScore = 0;
        for (String endpoint : endpoints) {
            long currentScore = HashSupport.hash64(key, endpoint);
            if (selectedEndpoint == null || Long.compareUnsigned(currentScore, selectedScore) > 0) {
                selectedEndpoint = endpoint;
                selectedScore = currentScore;
            }
        }
        return selectedEndpoint;
    }

    @Override
    public List<String> endpoints() {
        return endpoints;
    }
}
