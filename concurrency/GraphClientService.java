package concurrency;

import com.amazon.browse.exception.DependencyOutageException;
import com.amazon.browse.marketplace.model.Marketplace;
import com.amazon.browse.marketplace.model.Region;
import com.amazon.browse.spring.date.DateUtilsService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Responsible for returning client handles for graph operations.
 */
public abstract class GraphClientService<T> implements AutoCloseable {

    private static final int MINUTES_BEFORE_CLIENT_REFRESH = 5;

    private final Map<Region, LocalDateTime> timestamps = new ConcurrentHashMap<>();
    private Map<Region, T> clients = new HashMap<>();

    @Resource(name = "graphLocations")
    private Map<Region, String> graphLocations;

    @Autowired
    private DateUtilsService dateUtilsService;

    @PostConstruct
    private void postConstruct() {
        graphLocations.values().forEach(v -> {
            if (!new File(v).isFile()) {
                throw new DependencyOutageException(
                    "Graph file does not exist. Consider deploying BrowseGraphDataAgent. (" + v + ")");
            }
        });
    }

    protected abstract T refreshClient(Marketplace marketplace, T client);

    protected abstract void closeQuietly(T client);

    public T get(Marketplace marketplace) {
        LocalDateTime lastTimestamp = getLastTimestamp(marketplace);

        if (!clients.containsKey(marketplace.getRegion()) ||
            lastTimestamp.plusMinutes(MINUTES_BEFORE_CLIENT_REFRESH).isBefore(dateUtilsService.now())) {
            // Refresh of client is needed.
            T client = refreshClient(marketplace, clients.get(marketplace.getRegion()));
            clients.put(marketplace.getRegion(), client);
            timestamps.put(marketplace.getRegion(), dateUtilsService.now());
        }

        return clients.get(marketplace.getRegion());
    }

    @Override
    public void close() {
        for (T client : clients.values()) {
            closeQuietly(client);
        }
        clients.clear();
    }

    protected String getGraphLocation(Marketplace marketplace) {
        return graphLocations.get(marketplace.getRegion());
    }

    private LocalDateTime getLastTimestamp(Marketplace marketplace) {
        Region region = marketplace.getRegion();
        if (!timestamps.containsKey(region)) {
            timestamps.put(region, LocalDateTime.MIN);
        }

        return timestamps.get(region);
    }
}