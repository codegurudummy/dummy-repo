package concurrency;

import com.amazon.aaa.*;
import com.amazon.aaa.config.DualReadAndDualWriteConfig;
import com.amazon.aaa.dynamodb.common.model.Application;
import com.amazon.aaa.dynamodb.common.model.ApplicationEntity;
import com.amazon.aaa.storage.DBAccessor;
import com.amazon.aaa.storage.Factory;
import com.amazon.aaa.storage.SearchByOwnerThreadPool;
import com.amazon.aaa.storage.dynamo.application.ApplicationByApplicationIdGetter;
import com.amazon.aaa.storage.requests.HashAndRangeKeyRequest;
import com.amazon.aaa.storage.requests.applicationentity.SearchEverythingRequest;
import com.amazon.aaa.storage.requests.approval.LatestVersionRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Searches application entities.  The current implementation will
 * only return the first page of results.
 */
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ApplicationEntitiesSearcher {
    private final SearchFactory searchFactory;
    private final ApplicationByApplicationIdGetter applicationByApplicationIdGetter;
    private final Factory<DualReadAndDualWriteConfig> configFactory;
    private final SearchByOwnerThreadPool searchByOwnerThreadPool;

    public List<SearchEverythingMatch> searchEverything(final SearchEverythingRequest request) {
        final ArrayList<SearchEverythingMatch> results = new ArrayList<>();

        results.addAll(searchEverything(request.getQuery1()));
        results.addAll(searchEverything(request.getQuery2()));

        return results;
    }

    private Collection<? extends SearchEverythingMatch> searchEverything(final String searchableValue) {
        final ApplicationEntitiesSearcherByValue applicationEntitiesSearcherByValue =
                this.searchFactory.createApplicationEntitiesSearcherByValue();
        applicationEntitiesSearcherByValue.setLimit(DBAccessor.DEFAULT_SEARCH_RESULTS_LIMIT);

        final List<ApplicationEntity> results = applicationEntitiesSearcherByValue.apply(HashAndRangeKeyRequest.<String, String>builder()
                .hashKey(searchableValue)
                .rangeKey(ApplicationEntity.createStatusAndType(Status.Active, null))
                .build());

        return results.stream().map(this::mapSearchEverything).collect(Collectors.toList());
    }

    private SearchEverythingMatch mapSearchEverything(final ApplicationEntity entity) {
        return SearchEverythingMatch.builder()
                .withApplicationName(entity.getApplicationName())
                .withMatchType(entity.getType())
                .withApplicationId(entity.getApplicationID())
                .withMatchValue(entity.getValue())
                .withStatus(entity.getStatus())
                .build();
    }

    public List<SearchClientsMatch> searchClients(final SearchRequest request) {
        return searchOneOrByOwners(request, ApplicationEntityType.NotificationClientName)
                .parallelStream()
                .map(e->this.mapClients(e, new ConcurrentHashMap<String, Application>()))
                .collect(Collectors.toList());
    }

    public List<SearchServicesMatch> searchServices(final SearchRequest request) {
        return searchOneOrByOwners(request, ApplicationEntityType.NotificationServiceName)
                .parallelStream()
                .map(e->this.mapServices(e, new ConcurrentHashMap<String, Application>()))
                .collect(Collectors.toList());
    }

    public List<SearchApplicationsMatch> searchApplications(final SearchRequest request) {
        return searchOneOrByOwners(request, ApplicationEntityType.ApplicationName)
                .parallelStream()
                .map(e->this.mapApplications(e, new ConcurrentHashMap<String, Application>()))
                .collect(Collectors.toList());
    }

    private Collection<ApplicationEntity> searchOneOrByOwners(final SearchRequest request, final String type) {
        final PaginationStatus paginationStatus = request.getPaginationStatus();
        final Collection<ApplicationEntity> results;

        if(request.getSearchableValue().isEmpty()) {
            results = searchByOwners(request, type);
        } else {
            //There will always be only one result when a searchableValue is populated
            results = searchSingle(request, type);

            if(this.configFactory.create().isDynamoDBAuthoritative()) {
                paginationStatus.setTotalResultCount(results.size());
            }
        }

        return results;
    }

    private Collection<ApplicationEntity> searchSingle(final SearchRequest request, final String type) {
        final ApplicationEntitiesSearcherByValue applicationEntitiesSearcherByValue =
                searchFactory.createApplicationEntitiesSearcherByValue();
        applicationEntitiesSearcherByValue.setLimit(1);

        final HashAndRangeKeyRequest<String, String> currentSearchRequest =
                HashAndRangeKeyRequest.<String, String>builder()
                        .hashKey(request.getSearchableValue())
                        .rangeKey(ApplicationEntity.createStatusAndType(Status.Active, type))
                        .build();

        final ApplicationEntity result = applicationEntitiesSearcherByValue.apply(currentSearchRequest)
                .stream().findFirst().orElse(null);

        if(valid(result, request)) {
            return Collections.singletonList(result);
        }

        return Collections.emptyList();
    }

    private boolean valid(final ApplicationEntity result, final SearchRequest request) {
        return result!=null && (request.isIgnoreOwners()
                                    || validOwners(request, result));
    }

    private boolean validOwners(final SearchRequest searchRequest, final ApplicationEntity match) {
        final Set<String> ownersSet = searchRequest.getSearchableOwners().stream()
                .map(String::toLowerCase).collect(Collectors.toSet());

        return ownersSet.contains(match.getOwnerIndexHashKey());
    }

    private Collection<ApplicationEntity> searchByOwners(final SearchRequest request, final String type) {
        final PaginationStatus paginationStatus = request.getPaginationStatus();
        final ApplicationEntitiesSearcherByOwner searchByOwner =
                searchFactory.createApplicationEntitiesSearcherByOwner();

        final int limit;

        if(request.isIncludeAll()) {
            limit = Integer.MAX_VALUE;
        } else {
            limit = Math.min(paginationStatus.getCurrentResultsPerPage(), 500);
        }

        searchByOwner.setLimit(limit);

        final Collection<ApplicationEntity> results = new ArrayList<>();

        final List<Iterator<ApplicationEntity>> ownersResults = this.searchByOwnerThreadPool.execute(request.getOwners(), o ->
                searchByOwner.apply(HashAndRangeKeyRequest.<PermissionGroup, String>builder()
                        .hashKey(o)
                        .rangeKey(ApplicationEntity.createStatusAndType(Status.Active, type))
                        .build()).iterator());

        for(final Iterator<ApplicationEntity> ownersIterator : ownersResults) {
            while(ownersIterator.hasNext() && results.size()<limit) {
                results.add(ownersIterator.next());
            }
        }

        final Integer zeroBasedPage = paginationStatus.getCurrentPageNumber()-1;
        final Integer resultsPerPage = paginationStatus.getCurrentResultsPerPage();

        if(this.configFactory.create().isDynamoDBAuthoritative()) {
            paginationStatus.setTotalResultCount(results.size());
        }

        return results.stream()
                .skip(zeroBasedPage*resultsPerPage)
                .limit(resultsPerPage)
                .collect(Collectors.toList());
    }

    private SearchApplicationsMatch mapApplications(final ApplicationEntity entity,
                                                    final ConcurrentHashMap<String, Application> applications) {
        final Application application =
                getApplication(entity.getApplicationID(), applications);

        return SearchApplicationsMatch.builder()
                .withPermissionGroup(PermissionGroup.builder()
                        .withPermissionGroupName(entity.getOwnedByPermissionGroupName())
                        .withPermissionGroupType(entity.getOwnedByPermissionGroupType().toUpperCase())
                        .build())
                .withApplicationId(entity.getApplicationID())
                .withApplicationName(entity.getApplicationName())
                .withApplicationVersion(application.getApplicationVersion())
                .withApplicationStatus(entity.getStatus())
                .build();
    }

    private SearchServicesMatch mapServices(final ApplicationEntity entity,
                                            final ConcurrentHashMap<String, Application> applications) {
        final Application application =
                getApplication(entity.getApplicationID(), applications);

        return SearchServicesMatch.builder()
                .withApplicationId(entity.getApplicationID())
                .withApplicationVersion(application.getApplicationVersion())
                .withApplicationName(entity.getApplicationName())
                .withServiceStatus(entity.getStatus())
                .withRelationshipType(DBAccessor.
                        convertApplicationEntityTypeToRelationshipType(entity.getType()))
                .withServiceName(entity.getValue())
                .build();
    }

    private SearchClientsMatch mapClients(final ApplicationEntity entity,
                                          final ConcurrentHashMap<String, Application> applications) {
        final Application application =
                getApplication(entity.getApplicationID(), applications);

        return SearchClientsMatch.builder()
                .withApplicationId(entity.getApplicationID())
                .withApplicationVersion(application.getApplicationVersion())
                .withApplicationName(entity.getApplicationName())
                .withClientStatus(entity.getStatus())
                .withRelationshipType(DBAccessor.convertApplicationEntityTypeToRelationshipType(entity.getType()))
                .withClientName(entity.getValue())
                .build();
    }

    /**
     * Gets an application that was already retrieved or from the database.
     * @param applicationID
     * @param applications
     * @return
     */
    private Application getApplication(final String applicationID, final ConcurrentHashMap<String, Application> applications) {
        if(!applications.containsKey(applicationID)) {
            final Application application = this.applicationByApplicationIdGetter
                    .apply(LatestVersionRequest.<String>builder()
                            .request(applicationID)
                            .build());
            applications.put(applicationID,application);
        }

        return applications.get(applicationID);
    }
}