package concurrency;

import com.amazon.aws.ccs.persistence.DomainManager;
import com.amazon.aws.ccs.persistence.exceptions.PersistenceException;
import com.amazon.coral.metrics.Metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class MemDBDomainManager implements DomainManager<MemDBDomain> {

  private final Map<String, MemDBDomain> domains;
  private final String namespace;

  public MemDBDomainManager(String namespace) {
    this.namespace = namespace;
    this.domains = new ConcurrentHashMap<>();

  }

  @Override
  public MemDBDomain create(MemDBDomain domain, boolean waitUntilReady, boolean failIfExists) throws PersistenceException,
      TimeoutException {

    this.domains.put(domain.getName(), domain);

    return domain;
  }

  @Override
  public void configureDomain(MemDBDomain domain, Map<String, String> properties) throws TimeoutException {
    // noop
  }

  @Override
  public synchronized MemDBDomain retrieve(String domainName) throws PersistenceException {
    return this.domains.get(domainName);
  }

  @Override
  public synchronized void delete(String domainName) throws PersistenceException {
    MemDBDomain domainToDelete = this.domains.get(domainName);
    if (domainToDelete != null) {
      this.domains.remove(domainName);
    }
  }

  @Override
  public synchronized Map<String, MemDBDomain> getDomains() {
    return this.domains;
  }

  @Override
  public void reportMetrics(Metrics metrics) throws PersistenceException {
    // noop    
  }

}