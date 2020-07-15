package concurrency;

import amazon.PARIS.PARISService;
import amazon.PARIS.SORMerchantMP;
import amazon.deliveryservice.types.gdofi.DeliveryOption;
import amazon.product.dexaggregator.model.*;
import com.amazon.afs.common.utils.IdentifierUtil;
import com.amazon.afs.i18n.Currency;
import com.amazon.afscommon.Identifier;
import com.amazon.afsorderservice.*;
import com.amazon.commons.model.Aspect;
import com.amazon.commons.model.EntityType;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.dm.document.Document;
import com.amazon.dm.document.Entity;
import com.amazon.dm.document.query.EntityCollection;
import com.amazon.wl.utils.GsonProvider;
import com.google.gson.Gson;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Log4j2
@Service("AFSOrderService")
public class GetProductPromiseActivity extends Activity {
    protected static final EntityType DELIVERY_ENTITY_TYPE = new EntityType(
            new Aspect("Delivery"), "ItemDeliveryOption", "2.0");
    private static final int MAX_NUM_OF_DELIVERY_OPTIONS = 3;
    private static final Gson GSON = GsonProvider.getDefault();

    private ProductDeliveryExperienceAggregatorService pdexAggregatorServiceClient;
    private PARISService parisService;

    @Autowired
    public GetProductPromiseActivity(ProductDeliveryExperienceAggregatorService pdexAggregatorServiceClient,
                                     PARISService parisService) {
        this.pdexAggregatorServiceClient = pdexAggregatorServiceClient;
        this.parisService = parisService;
    }

    @Validated
    @Operation("GetProductPromise")
    public GetProductPromiseResponse enact(GetProductPromiseRequest input) {
        log.traceEntry("Get product promise input: {}", GSON.toJson(input));
        try {
            Identifier marketplaceId = IdentifierUtil.seed(input.getMarketplaceId());
            Identifier merchantId = IdentifierUtil.seed(input.getMerchantId());
            String encryptedMarketplaceId = marketplaceId.getEncryptedId();
            String encryptedMerchantId = merchantId.getEncryptedId();

            Validate.notEmpty(input.getAsin());
            Validate.notEmpty(input.getOfferListingId());
            Validate.notEmpty(input.getOfferSKU());

            SORMerchantMP sellerOfRecordForMerchant = getSellerOfRecordForMerchant(encryptedMarketplaceId, encryptedMerchantId);

            GetProductDeliveryInformationSummary.Request request = wrapGetProductDeliveryInformationSummaryInput(
                    input, sellerOfRecordForMerchant);

            ProductDeliveryInformationSummary productDeliveryInformationSummary;
            try {
                productDeliveryInformationSummary = request.call();
            } catch (InvalidParameterException e) {
                throw new InvalidInputException(e);
            } catch (Exception e) {
                throw new DependencyException(e);
            }
            return log.traceExit(convertProductDeliveryInformationSummary2Response(productDeliveryInformationSummary));
        } catch (InvalidInputException | IllegalArgumentException e) {
            log.error("GetProductPromise encounter invalid input exception", e);
            throw new InvalidInputException(e);
        } catch (DependencyException e) {
            log.error("GetProductPromise encounter dependency exception.", e);
            throw e;
        } catch (Exception e) {
            log.error("GetProductPromise Exception", e);
            throw new AFSException(e);
        }
    }

    protected GetProductPromiseResponse convertProductDeliveryInformationSummary2Response(
            ProductDeliveryInformationSummary productDeliveryInformationSummary) {
        GetProductPromiseResponse result = GetProductPromiseResponse.builder()
                .withValidDeliveryOptions(new ArrayList<>())
                .build();
        if (null == productDeliveryInformationSummary
                || null == productDeliveryInformationSummary.getValidDeliveryOptions()) {
            return result;
        }
        Map<String, DeliveryOptionSummary> id2DeliveryOptionSummary = productDeliveryInformationSummary
                .getValidDeliveryOptions();
        result.setDefaultDeliveryOption(productDeliveryInformationSummary.getDefaultDeliveryOption());
        result.getValidDeliveryOptions().addAll(id2DeliveryOptionSummary.values().stream()
                .filter(summary -> Objects.nonNull(summary) && Objects.nonNull(summary.getDeliveryOption()))
                .map(this::parseItemDeliveryOptionFromDeliveryOptionSummary)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        if (null != productDeliveryInformationSummary.getFastTrackSummary()) {
            result.setFastTrackDeliveryOptionIds(
                    productDeliveryInformationSummary.getFastTrackSummary().getFastTrackDeliveryOptions());
        }
        return result;
    }

    protected ItemDeliveryOption parseItemDeliveryOptionFromDeliveryOptionSummary(
            DeliveryOptionSummary deliveryOptionSummary) {
        DeliveryOption deliveryOption = parseDeliveryOptionFromDeliveryOptionSummary(deliveryOptionSummary);
        if (null == deliveryOption
                || null == deliveryOption.getPromise()
                || null == deliveryOption.getPromise().getPromiseQuality()
                || null == deliveryOption.getShipOption()
                || null == deliveryOption.getShipOption().getTransitTimeRange()) {
            return null;
        }
        Currency chargeFee = parseChargeFeeFromDeliveryServiceShippingCharge(deliveryOptionSummary.getShippingCharge());
        Promise promise = parsePromiseFromDeliveryServicePromise(deliveryOption.getPromise());
        ShipOption shipOption = parseShipOptionFromDeliveryServiceShipOption(
                deliveryOption.getShipOption());

        return ItemDeliveryOption.builder()
                .withDeliveryOptionId(deliveryOption.getDeliveryOptionId())
                .withPromise(promise)
                .withShipOption(shipOption)
                .withChargeFee(chargeFee)
                .withValidUntilDateInSecondsSinceEpoch(deliveryOption.getValidUntilDateInSecondsSinceEpoch())
                .build();
    }

    protected DeliveryOption parseDeliveryOptionFromDeliveryOptionSummary(DeliveryOptionSummary deliveryOptionSummary) {
        try {
            Document deliveryOptionDocument = (Document) deliveryOptionSummary.getDeliveryOption().getDocument(Document.class);
            deliveryOptionDocument.getDocumentFactory().registerCodigoClass(DeliveryOption.class,
                    DELIVERY_ENTITY_TYPE);
            EntityCollection entities = deliveryOptionDocument.selectEntities()
                    .byEntityTypes(DELIVERY_ENTITY_TYPE);

            /** There's only one ItemDeliveryOption entity in the
             *  document returned by PDEX.
             */
            Entity entity = entities.single();
            if (entity == null) {
                return null;
            }
            Object object = entity.asCodigoObject();
            return (object == null) ? null : (DeliveryOption) object;
        } catch (Exception e) {
            log.error("Error when parse document.", e);
            return null;
        }
    }

    private ShipOption parseShipOptionFromDeliveryServiceShipOption(
            amazon.deliveryservice.types.common.ShipOption shipOption) {
        DurationRange durationRange = com.amazon.afsorderservice.DurationRange.builder()
                .withMaxDurationInSeconds(shipOption.getTransitTimeRange().getMaxDurationInSeconds())
                .withMinDurationInSeconds(shipOption.getTransitTimeRange().getMinDurationInSeconds())
                .build();
        return ShipOption.builder()
                .withShipOptionKey(shipOption.getShipOptionKey())
                .withShippingSpeedCategory(shipOption.getShipCategory())
                .withTransitTimeRange(durationRange)
                .withIsSuperSaverShippingSupported(shipOption.getIsSuperSaverShippingSupported())
                .withSupportsCODDelivery(shipOption.getSupportsCODDelivery())
                .withSupportsRemoteFulfillment(shipOption.getSupportsRemoteFulfillment())
                .build();
    }

    private Promise parsePromiseFromDeliveryServicePromise(amazon.deliveryservice.types.common.Promise promise) {
        DateTimeRange timeOfDayRange = null;
        if (null != promise.getDeliveryTimeOfDayRange()) {
            timeOfDayRange = DateTimeRange.builder()
                    .withStartDateTimeInSecondsSinceEpoch(promise.getDeliveryTimeOfDayRange()
                            .getStartSecondsSinceBeginningOfDay())
                    .withEndDateTimeInSecondsSinceEpoch(promise.getDeliveryTimeOfDayRange()
                            .getEndSecondsSinceBeginningOfDay())
                    .build();
        }
        return Promise.builder()
                .withPromiseQuality(promise.getPromiseQuality().getValue())
                .withDeliveryDateRange(parseDeliveryDateTimeRange(promise.getDeliveryDateRange()))
                .withShippingDateRange(parseDeliveryDateTimeRange(promise.getShippingDateRange()))
                .withDeliveryTimeOfDayRange(timeOfDayRange)
                .withIsReleaseDatePromise(promise.getIsReleaseDatePromise())
                .build();
    }

    protected Currency parseChargeFeeFromDeliveryServiceShippingCharge(amazon.platform.types.Currency charge) {
        if (null == charge) {
            return null;
        }
        return Currency.builder().withValue(charge.getAmount()).withCode(charge.getCode()).build();
    }

    protected DateTimeRange parseDeliveryDateTimeRange(amazon.deliveryservice.types.common.DateTimeRange dateRange) {
        if (null == dateRange) {
            return null;
        }
        return DateTimeRange.builder()
                .withStartDateTimeInSecondsSinceEpoch(dateRange.getStartDateTimeInSecondsSinceEpoch())
                .withEndDateTimeInSecondsSinceEpoch(dateRange.getEndDateTimeInSecondsSinceEpoch())
                .build();
    }

    protected GetProductDeliveryInformationSummary.Request wrapGetProductDeliveryInformationSummaryInput(
            GetProductPromiseRequest input,
            SORMerchantMP sellerOfRecordForMerchant) {
        Validate.notNull(input);
        Validate.notNull(sellerOfRecordForMerchant);
        OfferListingInformation offerListingInformation = new OfferListingInformation();
        offerListingInformation.setOfferListingID(input.getOfferListingId());

        CatalogInformation catalogInformation = new CatalogInformation();
        catalogInformation.setASIN(input.getAsin());
        catalogInformation.setOfferSKU(input.getOfferSKU());

        Product product = new Product();
        product.setOfferListingInformation(offerListingInformation);
        product.setCatalogInformation(catalogInformation);

        SellerInformation sellerInformation = new SellerInformation();
        sellerInformation.setMarketplaceID(input.getMarketplaceId().getEncryptedId());
        sellerInformation.setMerchantID(input.getMerchantId().getEncryptedId());
        sellerInformation.setMerchantCustomerID(sellerOfRecordForMerchant.getSORMerchant().getMerchantCustomerID());
        sellerInformation.setSellerOfRecordID(sellerOfRecordForMerchant.getSORMerchant().getSOR().getLegalEntityID());

        GetProductDeliveryInformationSummary.Request request = pdexAggregatorServiceClient
                .newGetProductDeliveryInformationSummaryRequest(product, sellerInformation);
        request.setMaximumDeliveryOptionCount(MAX_NUM_OF_DELIVERY_OPTIONS);
        if (input.getDestination() != null) {
            com.amazon.afsorderservice.Address inputDestination = input.getDestination();
            Address address = new Address();
            address.setCountryCode(inputDestination.getCountryCode());
            address.setPostalCode(inputDestination.getPostalCode());
            address.setStateOrRegion(inputDestination.getStateOrProvinceCode());
            address.setCity(inputDestination.getCity());
            address.setDistrictOrCounty(inputDestination.getDistrict());
            request.setDestination(address);
        }
        return request;
    }

    /**
     * This map is to store sellerOfOrderRecord result and avoid redundant duplicated call to PARIS.
     * For now we're not sure how many merchants will there be, just assume the merchant count is
     * not large and thus use this temporary way as a cache.
     * <p>
     * When merchant count get larger, will change to use standard caching technologies in
     * https://w.amazon.com/index.php/CachingTeam
     */
    private static ConcurrentHashMap<Pair<String, String>, SORMerchantMP> sorMerchantMPMap = new ConcurrentHashMap<>();

    /**
     * @param marketplaceId      the encrypted marketplaceId
     * @param merchantCustomerId the encrypted merchant customerId
     * @return the seller of record response from PARIS
     * @throws Exception when the call fails
     */
    protected SORMerchantMP getSellerOfRecordForMerchant(
            final String marketplaceId,
            final String merchantCustomerId) {
        try {
            Pair<String, String> keyPair = Pair.of(marketplaceId, merchantCustomerId);
            SORMerchantMP sorMerchantMP = sorMerchantMPMap.get(keyPair);
            if (null == sorMerchantMP) {
                sorMerchantMP = parisService.getDefaultSORByMerchantMP(marketplaceId, merchantCustomerId);
                sorMerchantMPMap.put(keyPair,sorMerchantMP);
            }
            return sorMerchantMP;
        } catch (amazon.PARIS.InvalidInputException | amazon.PARIS.InvalidIDException e) {
            throw new InvalidInputException(e);
        } catch (Exception e) {
            throw new DependencyException(
                    "Unable to retrieve seller of record id for marketplaceId: " + marketplaceId
                            + "merchantId: " + merchantCustomerId, e);
        }
    }
}