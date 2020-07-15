package com.amazon.ingcfn.order;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import amazon.platform.config.AppConfig;

import com.amazon.commons.model.Document;
import com.amazon.commons.model.DocumentException;
import com.amazon.commons.model.DocumentFactory;
import com.amazon.commons.model.Entity;
import com.amazon.commons.model.identifier.MIQ;
import com.amazon.commons.request.RequestHandlerException;
import com.amazon.commons.util.Anything;
import com.amazon.commons.util.AnythingList;
import com.amazon.helpers.serialization.SerializationUtils;
import com.amazon.herd.service.entity.LegacyPriorEvent;
import com.amazon.ingc.common.HerdDocumentConstants;
import com.amazon.ingc.common.utils.DateUtility;
import com.amazon.ingc.db.model.DeliveryEntity;
import com.amazon.ingc.db.model.LineItemEntity;
import com.amazon.ingc.db.model.enums.OrderStatus;
import com.amazon.ingcfn.common.Constants.APPLICATION;
import com.amazon.ingcfn.common.Constants.CONFIG_KEYS;
import com.amazon.ingcfn.common.MetricConstants.LATENCY_METRIC;
import com.amazon.ingcfn.common.enums.GCFNOrderStatus;
import com.amazon.ingcfn.common.helper.CommonHelper;
import com.amazon.ingcfn.common.helper.OrderEventAppender;
import com.amazon.ingcfn.converter.OrderLineItemToShipmentLineItemConverter;
import com.amazon.ingcfn.converter.ShipmentLineItemToLineItemEntityConverter;
import com.amazon.ingcfn.dao.CachePersistenceProvider;
import com.amazon.ingcfn.dao.DatabasePersistenceProvider;
import com.amazon.ingcfn.exception.GCApplicationException;
import com.amazon.ingcfn.exception.GCSystemException;
import com.amazon.ingcfn.model.DeliveryLineItem;
import com.amazon.ingcfn.model.GCCustomizationData;
import com.amazon.ingcfn.model.db.Order;
import com.amazon.ingcfn.model.db.ShipmentLineItem;
import com.amazon.ingcfn.model.enums.DeliveryLineItemStatus;
import com.amazon.ingcfn.model.enums.ShipmentStatus;
import com.amazon.ingcfn.model.odm.OrderInformation;
import com.amazon.ingcutils.common.model.enums.TokenatorDataTypes;
import com.amazon.ingcutils.connector.HerdConnector;
import com.amazon.ingcutils.connector.SaganConnector;
import com.amazon.ingcutils.connector.TokenatorConnector;
import com.amazon.ingcutils.exception.HerdException;
import com.amazon.ingcutils.exception.TokenatorException;
import com.amazon.ingcutils.metrics.MetricsProvider;
import com.amazon.sagan.SequenceException;
import com.amazon.shared.model.cow.enums.OrderStateType;
import com.amazon.shared.model.cow.herd.FulfillmentRequestMessageType;

/**
 * Orchestrator to assemble multiple service calls to process the given request
 *
 *
 */
@Component
public class GCFulfillmentOrchestrator {

    private static final Logger LOG = (Logger) LogManager
            .getLogger(GCFulfillmentOrchestrator.class);

    public final String OBJECT_ID_PREFIX = AppConfig
            .findString(APPLICATION.APP_NAME + "."
                    + CONFIG_KEYS.OBJECT_ID_PREFIX);
    public final String SHIP_GEN_INSTRUCTION_ID = AppConfig
            .findString(APPLICATION.APP_NAME + "."
                    + CONFIG_KEYS.SHIP_GEN_INSTRUCTION_ID);
    public final String SHIPMENT_REQUEST_NOTIFICATION_EVENT_NAME = AppConfig
            .findString(APPLICATION.APP_NAME + "."
                    + CONFIG_KEYS.SHIPGEN_REQUEST_NOTIFICATION_EVENT);
    private final String HERD_CLIENT_ID = AppConfig
            .findString(APPLICATION.APP_NAME + "." + CONFIG_KEYS.HERD_CLIENT_ID);

    private final String SHIPGEN_CONTEXT = AppConfig
            .findString(APPLICATION.APP_NAME + "."
                    + CONFIG_KEYS.SHIPGEN_CONTEXT);

    private static final ZoneId DELIVERY_TIMEZONE_ID = ZoneId
            .of(AppConfig.findString(APPLICATION.APP_NAME + "." + CONFIG_KEYS.DELIVERY_TIMEZONE));


    @Autowired
    private CachePersistenceProvider cpProvider;

    @Autowired
    private DatabasePersistenceProvider dbProvider;

    @Autowired
    HerdConnector herdConnector;

    @Autowired
    private SaganConnector saganConnector;

    @Autowired
    private MetricsProvider metricProvider;

    @Autowired
    private TokenatorConnector tokenator;

    @Autowired
    private OrderEventAppender orderEventAppender;

    @Autowired
    private Clock deliveryTimeZoneClock;

    /**
     * Processes the given order request. Responsible to talk to persistent storage and also contains the actual
     * business logic needed prior to FRID generation for the given request. Will also generate the FRID for this
     * request and that is considered as a response.
     *
     * @param sequenceIDprocessShipGenRequestSuccess
     * @param marketPlace
     * @param orderStateType
     * @param orderDetails
     * @throws IOException
     * @throws GCApplicationException
     * @throws NumberFormatException
     * @throws GCSystemException
     */
    public void processAssignedOrder(String sequenceID, String marketPlace,
                                     String orderStateType, OrderInformation orderDetails)
            throws IOException, NumberFormatException, GCApplicationException,
            GCSystemException {

        metricProvider.startTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_ASSIGNED);

        // validate if the request is out of order. If it is then this request
        // should be ignored. Currently we are not
        // saving the out of order requests
        String orderID = orderDetails.getExternalOrderID();

        LOG.debug("Entering processAssignedOrder() for orderId : " + orderID
                + " & sequenceId : " + sequenceID);

        if (isOutOfOrderRequest(Double.valueOf(sequenceID), orderID)) {
            String message = "The request is out of order hence ignoring the same:"
                    + sequenceID
                    + " "
                    + orderStateType
                    + " "
                    + orderDetails.getExternalOrderID();
            LOG.info(message);
            metricProvider
                    .endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_ASSIGNED);
            return;
        }

        // attempt to persist the order to the persistent cache. For the same
        // order with the same sequence no you should get the
        // same Persistence response

        orderDetails = cpProvider.persistOrderInformation(
                Long.valueOf(sequenceID), orderID, orderDetails);

        LOG.info("Order status for the given orderID:" + orderID + " is:"
                + orderDetails.getOpStatus());
        switch (orderDetails.getOpStatus()) {
            case COMPLETED: {
                LOG.info("This request has already been completed.Not sending the shipgen request for the same order:"
                        + orderDetails.getOpStatus());
                metricProvider
                        .endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_ASSIGNED);
                return;
            }
            case PROCESSING: {
                // Check the creation time. if the creation time and current
                // time difference is more than threshold time
                LOG.info("Order is in 'PROCESSING' state. Process if creation time & current time difference is more than threshold time");

                if (isExpiredResponse(orderDetails)) {
                    // TODO make the status as NEW then retry the process for this
                    // request
                    LOG.info("Creation time & current time difference is more than threshold time. Re Processing order : "
                            + orderID);
                    processNewOrderRequest(orderID, orderDetails);
                } else {
                    LOG.info("INGCFN is currently processing the request");
                    metricProvider
                            .endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_ASSIGNED);
                    throw new GCApplicationException("INGCFN is currently processing the request for OrderId:" + orderID);
                }
                break;
            }
            case NEW: {
                LOG.info("Order is in 'NEW' state. Processing order : " + orderID);
                processNewOrderRequest(orderID, orderDetails);
                break;
            }
            case FAILED: {
                // The previous request for the processing this order details has
                // failed and now it needs to be started
                // afresh
                LOG.info("Previous request for this order is in 'FAILED' state. Processing order : "
                        + orderID);
                cpProvider.updateOrderStatus(Long.valueOf(sequenceID), orderID,
                        GCFNOrderStatus.NEW);
                processNewOrderRequest(orderID, orderDetails);

                break;
            }
            default:
                break;
        }

        metricProvider.endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_ASSIGNED);
        LOG.debug("Exiting processAssignedOrder() for orderId : " + orderID
                + " & sequenceId : " + sequenceID);

    }

    /**
     * Process shipGen Request and invoke corresponding COW workflow with required arguments. Herd document will be
     * created with required entity and LegacyPriorEvent. Once all fields are ready, HERD work item will be opened
     * through CowConnectorService.
     *
     * @param shipmentId
     * @param orderId
     * @param marketPlaceId
     * @return
     * @throws DocumentException
     * @throws GCApplicationException
     * @throws RequestHandlerException
     * @throws GCSystemException
     */
    public void processShipGenRequest(String shipmentId, String orderId,
                                      long marketPlaceId, Anything producerData)
            throws GCApplicationException, GCSystemException {

        metricProvider.startTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_SHIPGEN);
        LOG.debug("Entering processShipGenRequest() for orderId : " + orderId
                + " & shipmentId : " + shipmentId);

        validateShipGenRequestInputs(shipmentId, orderId, marketPlaceId);

        if (producerData == null || !producerData.containsKey("items")
                || producerData.getAsAnythingList("items").isEmpty()) {
            String msg = "Producer data cannot be null and should have at least one shipment item";
            LOG.error(msg);
            metricProvider
                    .endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_SHIPGEN);
            throw new GCApplicationException(msg);
        }

        try {

            String objectId = OBJECT_ID_PREFIX + shipmentId;

            Document initialHerdDocument = new DocumentFactory()
                    .createDocument();
            initialHerdDocument.getDocumentFactory().registerCommonsClass(
                    FulfillmentRequestMessageType.class,
                    HerdDocumentConstants.EntityType.GCFN_FULFILLMENT_REQUEST);
            FulfillmentRequestMessageType frMessage = new FulfillmentRequestMessageType();
            frMessage.setOrderID(orderId);
            frMessage.setFulfillmentID(shipmentId);
            frMessage.setMarketplaceID(marketPlaceId);
            Entity entity = initialHerdDocument.addEntity(frMessage);
            entity.setIdentifier(MIQ.create(entity, SHIP_GEN_INSTRUCTION_ID
                    + "/" + objectId));
            initialHerdDocument.getDocumentFactory().registerCommonsClass(
                    LegacyPriorEvent.class, LegacyPriorEvent.ENTITY_TYPE);
            LegacyPriorEvent priorEvent = new LegacyPriorEvent(
                    SHIPMENT_REQUEST_NOTIFICATION_EVENT_NAME, producerData);

            initialHerdDocument.addEntity(priorEvent).setIdentifier(
                    priorEvent.newIdentifier());
            herdConnector.openWorkFlow(objectId, SHIP_GEN_INSTRUCTION_ID,
                    HERD_CLIENT_ID, initialHerdDocument, SHIPGEN_CONTEXT);

        } catch (DocumentException e) {
            LOG.error(e.getMessage());
            throw new GCApplicationException(e);
        } catch (HerdException e) {
            LOG.error(e.getMessage());
            if (e.isRetryable()) {
                throw new GCSystemException(e);
            } else {
                throw new GCApplicationException(e);
            }
        } finally {
            metricProvider
                    .endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_SHIPGEN);
        }

        LOG.debug("Exiting processShipGenRequest() for orderId : " + orderId
                + " & shipmentId : " + shipmentId);
    }

    private boolean isShipGenRequestSentForShipment(String shipmentID,
                                                    ShipmentStatus shipmentStatus) throws GCApplicationException {

        if (null == shipmentStatus) {
            String message = "Shipment Status is null for shipmentID:"
                    + shipmentID;
            LOG.error(message);
            throw new GCApplicationException(message);
        }

        LOG.info("Shipment Status is:" + shipmentStatus);

        if (!(ShipmentStatus.NEW.equals(shipmentStatus)
                || ShipmentStatus.SHIPGEN_REQUEST_SEND_FAILED
                .equals(shipmentStatus) || ShipmentStatus.SHIPGEN_FAILED
                .equals(shipmentStatus))) {

            return true;

        }
        return false;
    }

    /**
     * process the given order details and generates the shipments for this order. If the order details are present then
     * the shipment previously persisted will be retrieved. If not shipments are present for then the shipments are
     * added to it. If for the existing shipments the FRID is not present then that is generated and added
     *
     * @throws RequestHandlerException
     * @throws DocumentException
     * @throws NumberFormatException
     *
     */
    public List<ShipmentLineItem> getShipmentsForOrder(
            OrderInformation orderInfo) throws GCApplicationException {

        LOG.debug("Entering getShipmentsForOrder() for order : "
                + orderInfo.getExternalOrderID());

        List<ShipmentLineItem> newShipments = orderInfo.getOrderLineItems().stream().map(new OrderLineItemToShipmentLineItemConverter()).collect(Collectors.toList());

        String marketPlace = orderInfo.getMarketplaceID();

        // Check in database if there are shipments present for the given
        // orderID
        List<ShipmentLineItem> shipments = cpProvider
                .getShipmentsForOrder(orderInfo.getExternalOrderID());

        LOG.info("Total Shipments for this order:" + shipments.size());
        if (shipments.size() == 0) {
            LOG.info("Persisting new shipments for order:" + orderInfo.getExternalOrderID());
            shipments = addShipmentsForOrderForMarketPlace(orderInfo, newShipments, marketPlace);
        } else if (shipments.size() != newShipments.size()) {
            // We are not allowing shipment changes for the order.
            // The previous Shipments will be overwritten and replaced with new
            // Shipments if the expected shipment count
            // does not match the shipment count present in SABLE

            // Remove the existing shipments present
            LOG.info(
                    "The shipment written are dirty and incomplete.Removing existing and create new shipment line items");
            removeShipmentsForOrder(orderInfo.getExternalOrderID(), shipments);
            shipments = addShipmentsForOrderForMarketPlace(orderInfo, newShipments, marketPlace);
        }

        LOG.debug("Exiting getShipmentsForOrder() for order : "
                + orderInfo.getExternalOrderID() + " with " + shipments.size()
                + " shipments");
        return shipments;

    }

    private void removeShipmentsForOrder(String orderID,
                                         List<ShipmentLineItem> shipments) throws GCApplicationException {

        for (ShipmentLineItem toDelete : shipments) {
            String key = orderID + ":" + toDelete.getShipmentID();
            cpProvider.deleteShipmentsForOrder(key);
        }
    }

    private List<ShipmentLineItem> addShipmentsForOrderForMarketPlace(
            OrderInformation orderInfo, List<ShipmentLineItem> newShipments,
            String marketPlace) throws GCApplicationException {
        for (ShipmentLineItem newShipment : newShipments) {
            newShipment.setShipmentID(generateNewShipmentID(marketPlace));
        }
        // save the shipment line items in the cache
        cpProvider.addShipmentsForOrder(orderInfo.getExternalOrderID(),
                newShipments);
        return newShipments;
    }

    /**
     * Checks if the response is an expired which is the case when the request takes more time than the configuration
     * magic time interval
     *
     * @param response
     * @return
     */
    private boolean isExpiredResponse(OrderInformation response) {

        int timeDelay = 0;
        try {
            timeDelay = AppConfig
                    .findInteger(CONFIG_KEYS.DUPLICATE_PROCESSING_TIME_INTERVAL);
            if (timeDelay <= 0) {
                timeDelay = APPLICATION.DEFAULT_TIME_INTERVAL;
            }

        } catch (Exception e) {
            LOG.warn("duplicate processing delay not specified in cfg file"
                    + CONFIG_KEYS.DUPLICATE_PROCESSING_TIME_INTERVAL
                    + " Picking up default values");
            timeDelay = APPLICATION.DEFAULT_TIME_INTERVAL;
        }

        Date expirationTime = DateUtils.addMilliseconds(
                response.getGcfnEntryCreationTime(), timeDelay);
        Date currentTime = Calendar.getInstance().getTime();
        if (currentTime.getTime() > expirationTime.getTime()) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public boolean isOutOfOrderRequest(double sequenceID, String orderID)
            throws GCApplicationException {

        // Get top entries for the given order ID
        List<Double> sequence = cpProvider.getSequenceIDListForOrder(orderID);

        double maxSequence = 0d;
        for (Double element : sequence) {
            if (element.doubleValue() > maxSequence) {
                maxSequence = element.doubleValue();
            }
        }
        if (maxSequence > sequenceID) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    /**
     * Constructs Producer data for shipGen
     *
     * @param lineItem
     * @return
     */
    private Anything constructProducerData(ShipmentLineItem lineItem,
                                           OrderInformation order) {

        LOG.debug("Entering constructProducerData() for shipment : "
                + lineItem.getShipmentID());

        int totalQuantity = lineItem.getGcCustomizations().stream().mapToInt(GCCustomizationData::getQuantity).sum();

        Anything producerData = new Anything();
        AnythingList anythingList = new AnythingList();
        Anything anything = new Anything();
        anything.add("order_id", lineItem.getOrderID());
        anything.add("line_item_code", lineItem.getLineItemCode());
        anything.add("quantity", totalQuantity);
        anything.add("asin", lineItem.getAsin());
        anything.add("shipping_option", order.getShippingOption());
        anything.add("shipping_split_preference",
                order.getShippingSplitPreference());
        anything.add("shipping_address_id", order.getShippingAddressID());
        anything.add("ship_to_address_code", order.getShipToAddressCode());
        anythingList.add(anything);
        producerData.add("items", anythingList);
        producerData.add("Service", "INGCFNService");
        producerData.add("shipment_id", lineItem.getShipmentID());

        LOG.debug("Entering constructProducerData() for shipment : "
                + lineItem.getShipmentID());

        return producerData;
    }

    /**
     * To generate a new FRID
     *
     * @return String - FRID for the given request
     */
    private String generateNewShipmentID(String marketPlace)
            throws GCApplicationException {

        LOG.info("Generating new shipment ID for marketplace:" + marketPlace);

        long shipmentID = 0L;
        try {

            shipmentID = saganConnector.getNextShipmentId();

        } catch (SequenceException e) {

            String message = "Failed to get shipment ID from Sagan:"
                    + shipmentID + " Reason:" + e.getMessage();
            LOG.error(message, e);

            throw new GCApplicationException(message, e);
        }

        LOG.info("Generated new shipment ID for marketplace:" + marketPlace
                + " " + shipmentID);

        return CommonHelper.trimFractionFromNumberString(String
                .valueOf(shipmentID));
    }

    /**
     * Checks if COMPLETED Order can be found in DB and Cache and mark it as complete in both
     *
     * @param orderID
     * @param sequenceID
     * @param orderStatus
     * @throws GCSystemException
     */
    public void updateOrderStatus(String sequenceID, String orderID,
                                  String orderStatus) throws GCApplicationException,
            GCSystemException {
        LOG.debug("Entering updateOrderStatus() for orderId : " + orderID);
        GCFNOrderStatus toOrderStatus = null;
        if (OrderStateType.valueOf(orderStatus)
                .equals(OrderStateType.COMPLETED)) {
            toOrderStatus = GCFNOrderStatus.COMPLETED;
            cpProvider.updateOrderStatus(Long.valueOf(sequenceID), orderID,
                    toOrderStatus);
            dbProvider.updateOrderStatus(orderID, OrderStatus.COMPLETED);
            LOG.debug("Marked order " + orderID
                    + " as COMPLETED in Sable and DB");

        }
        LOG.debug("Exiting updateOrderStatus() for orderId : " + orderID);
    }

    private void processNewOrderRequest(String orderID,
                                        OrderInformation response) throws GCApplicationException,
            NumberFormatException, GCSystemException {

        metricProvider
                .startTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_NEW_ORDER);
        // process a new order request. Save order details in dynamo DB and
        // SABLE

        LOG.debug("Entering processNewOrderRequest() for orderId : " + orderID);

        saveNewOrder(response);

        List<ShipmentLineItem> shipments = getShipmentsForOrder(response);

        // Save the order line items in the database.
        saveOrderLineItems(shipments);

        Map<String, ShipmentLineItem> deliveryIdToShipmentLineItemMap = createDeliveryElementsForShipment(shipments,
                response.getMarketplaceID());

        orderEventAppender.addShipmentDetails(deliveryIdToShipmentLineItemMap);

        attemptShipgenForShipments(orderID, response, shipments);

        LOG.debug("Exiting processNewOrderRequest() for orderId : " + orderID);
        metricProvider.endTimer(LATENCY_METRIC.ORDER_UPDATE_PROCESS_NEW_ORDER);

    }

    private Map<String, ShipmentLineItem> createDeliveryElementsForShipment(List<ShipmentLineItem> shipments, String marketPlace) throws GCApplicationException {

        LOG.debug("Entering createDeliveryElementsForShipment()");
        Map<String, ShipmentLineItem> deliveryIdToShipmentLineItemMap = new HashMap<>();

        // For each shipment create one delivery item for a single quantity
        for(ShipmentLineItem shipment : shipments) {

            List<DeliveryLineItem> deliveryItemForShipment = new ArrayList<>();

            List<DeliveryEntity> deliveryEntityList = new ArrayList<>();

            for(GCCustomizationData gcCustomizationData : shipment.getGcCustomizations()) {

                for(int index = 0; index < gcCustomizationData.getQuantity(); index++) {

                    String deliveryId = generateNewShipmentID(marketPlace);

                    deliveryIdToShipmentLineItemMap.put(deliveryId, shipment);

                    DeliveryLineItem temp = new DeliveryLineItem();
                    temp.setDeliveryId(deliveryId);
                    temp.setStatus(DeliveryLineItemStatus.NEW);

                    deliveryItemForShipment.add(temp);

                    DeliveryEntity entity = new DeliveryEntity();
                    entity.setAddressId(" ");
                    entity.setDeliveryDate(Calendar.getInstance().getTime());
                    entity.setDeliveryId(deliveryId);
                    entity.setRecipientEmailToken(gcCustomizationData.getRecipientEmailId());
                    entity.setType(shipment.getDeliveryType());
                    entity.setDenomination(gcCustomizationData.getDenomination());
                    entity.setTemplateId(gcCustomizationData.getTemplateName());
                    entity.setSenderNameToken(gcCustomizationData.getPurchaserName());
                    entity.setMessageToken(gcCustomizationData.getMessage());
                    entity.setSubjectToken(generateEmailSubjectToken(gcCustomizationData.getPurchaserName()));
                    entity.setCardNumber(gcCustomizationData.getCardNumber());

                    if (gcCustomizationData.isScheduledDelivery()) {
                        final Date scheduledDate = DateUtility.convertLocalDateToDate(gcCustomizationData.getScheduledDate(), DELIVERY_TIMEZONE_ID);
                        entity.setDeliveryDate(scheduledDate);
                        entity.setScheduledDate(scheduledDate);
                        entity.setScheduledDelivery(true);
                    }else{
                        entity.setDeliveryDate(Date.from(deliveryTimeZoneClock.instant()));
                    }

                    deliveryEntityList.add(entity);
                    LOG.debug("Created delivery entity or shipment " + shipment.getShipmentID() + " : " + SerializationUtils.serialize(entity));
                }
            }

            // Get delivery item for the given shipment id
            List<DeliveryLineItem> existingDeliveryItems = cpProvider.getDeliveryLineItemsForShipment(shipment.getShipmentID());
            LOG.debug("Existing delivery items : " + SerializationUtils.serialize(existingDeliveryItems));
            if (existingDeliveryItems.size() == 0) {
                LOG.debug("No existing delivery records");
                LOG.info("Adding new delivery line items for shipment:" + shipment.getShipmentID());
                cpProvider.addDeliveryLineItemsForShipment(shipment.getShipmentID(), deliveryItemForShipment);
                LOG.debug("Saving delivery enities : " + SerializationUtils.serialize(deliveryEntityList));
                dbProvider.batchSaveEntities(deliveryEntityList);

            } else if (existingDeliveryItems.size() != deliveryItemForShipment.size()) {
                LOG.debug("Mismatch in delivery enities. Deleting existing entries and adding the delivery entries again");
                // There is a mismatch between the delivery items present and
                // the ones created now. remove the existing
                // and add the delivery items again
                for(DeliveryLineItem toDelete : deliveryItemForShipment) {
                    String key = shipment.getShipmentID() + ":" + toDelete.getDeliveryId();
                    cpProvider.deleteDeliveryItem(key);
                }
                cpProvider.addDeliveryLineItemsForShipment(shipment.getShipmentID(), deliveryItemForShipment);
                LOG.debug("Saving delivery entries : " + SerializationUtils.serialize(deliveryEntityList));
                dbProvider.batchSaveEntities(deliveryEntityList);
            }

        }
        return deliveryIdToShipmentLineItemMap;
    }

    private void saveNewOrder(OrderInformation orderInfo)
            throws GCApplicationException, GCSystemException {

        metricProvider.startTimer(LATENCY_METRIC.ORDER_UPDATE_SAVE_ORDERS);

        LOG.debug("Entering saveNewOrder()");

        // check if the order is already present. If found then the save should
        // not happen
        dbProvider.saveNewOrder(getNewOrder(orderInfo));

        metricProvider.endTimer(LATENCY_METRIC.ORDER_UPDATE_SAVE_ORDERS);

        LOG.debug("Entering exitingNewOrder()");

    }

    private void saveOrderLineItems(List<ShipmentLineItem> shipments)
            throws GCApplicationException {

        metricProvider.startTimer(LATENCY_METRIC.ORDER_UPDATE_SAVE_LINE_ITEM);

        LOG.debug("Entering saveOrderLineItems()");
        List<LineItemEntity> entityList = shipments.stream().map(new ShipmentLineItemToLineItemEntityConverter(tokenator)).collect(Collectors.toList());
        dbProvider.batchSaveEntities(entityList);

        metricProvider.endTimer(LATENCY_METRIC.ORDER_UPDATE_SAVE_LINE_ITEM);

        LOG.debug("Entering saveOrderLineItems()");
    }

    private Order getNewOrder(OrderInformation orderInfo) {

        LOG.debug("Entering getNewOrder():" + orderInfo);

        Order order = new Order();
        order.setMarketPlaceID(orderInfo.getMarketplaceID());
        order.setOrderID(orderInfo.getExternalOrderID());
        order.setPaymentDetails(orderInfo.getPaymentDetails());
        order.setPurchasingCustomerID(orderInfo.getPurcharserCustomerID());
        order.setPurchaserNameToken(orderInfo.getPurchaserNameToken());
        order.setPurchaserEmailToken(orderInfo.getPurchaserEmailToken());
        order.setPurchaserMobileToken(orderInfo.getPurchaserMobileToken());
        order.setPurchaserIPAddress(orderInfo.getPurchaserIPAddress());
        order.setOrderCreationDate(orderInfo.getOrderCreationDate());
        order.setBillingAddressId(orderInfo.getBillingAddressId());
        LOG.debug("Exiting getNewOrder()");

        return order;
    }

    /**
     * Generate subject for the email that will be delivered to the customer. Appends the senderName if present else
     * Default value is used. The subject is also tokenated and returned.
     *
     * @param senderName
     * @return
     * @throws GCApplicationException
     */

    private String generateEmailSubjectToken(String senderName) throws GCApplicationException {

        String subject = APPLICATION.DEFAULT_SUBJECT_MESSAGE;

        try {
            if (!StringUtils.isBlank(senderName)) {
                String plainTextSenderName = tokenator.resolveIfAToken(senderName);
                subject = subject.replace(APPLICATION.DEFAULT_SENDER_NAME, plainTextSenderName);
            }

            subject = tokenator.getToken(TokenatorDataTypes.PURCHASE_EMAIL_SUBJECT.getDataTypeValue(), subject);
        } catch (TokenatorException e) {
            throw new GCApplicationException("Failed to convert subject to token", e);
        }
        return subject;
    }

    private void attemptShipgenForShipments(String orderID,
                                            OrderInformation response, List<ShipmentLineItem> shipments)
            throws GCApplicationException, NumberFormatException,
            GCSystemException {

        LOG.debug("Entering attemptShipgenForShipments() for orderId : "
                + orderID + " & " + shipments.size() + " shipments");

        int successCounter = 0;
        // For each shipments send a shipgen request
        for (ShipmentLineItem lineItem : shipments) {
            // generate shipgen request for the same
            LOG.debug("Generating shipGen request for order : " + orderID
                    + " & shipment : " + lineItem.getShipmentID());

            try {

                if (!isShipGenRequestSentForShipment(lineItem.getShipmentID(),
                        lineItem.getShipmentStatus())) {

                    processShipGenRequest(lineItem.getShipmentID(),
                            lineItem.getOrderID(),
                            Long.valueOf(response.getMarketplaceID()),
                            constructProducerData(lineItem, response));
                    LOG.debug("Generated shipGen request for order : "
                            + orderID + " & shipment : "
                            + lineItem.getShipmentID());
                    cpProvider.updateShipmentStatus(lineItem.getOrderID(),
                            lineItem.getShipmentID(),
                            ShipmentStatus.SHIPGEN_REQUEST_SEND);
                } else {
                    LOG.warn(String
                            .format("ShipGen is already initiated or completed for the ShipmentLineItem with orderId : %s & shipmentId : %s",
                                    orderID, lineItem.getShipmentID()));
                }

                successCounter++;
            } catch (GCSystemException e) {
                String message = "Failed generate shipment request:"
                        + lineItem.getShipmentID() + " Reason:"
                        + e.getMessage();
                LOG.error(message, e);
                cpProvider.updateShipmentStatus(lineItem.getOrderID(),
                        lineItem.getShipmentID(),
                        ShipmentStatus.SHIPGEN_REQUEST_SEND_FAILED);
                cpProvider.updateOrderStatus(response.getSequenceID(),
                        response.getExternalOrderID(), GCFNOrderStatus.FAILED);
                throw e;
            } catch (GCApplicationException e) {
                String message = "Failed to generate shipment request:"
                        + lineItem.getShipmentID() + " Reason:"
                        + e.getMessage();
                LOG.error(message, e);
                LOG.info("Updating the status to SHIPMENT_SEND_FAILED");
                cpProvider.updateShipmentStatus(lineItem.getOrderID(),
                        lineItem.getShipmentID(),
                        ShipmentStatus.SHIPGEN_REQUEST_SEND_FAILED);
                cpProvider.updateOrderStatus(response.getSequenceID(),
                        response.getExternalOrderID(), GCFNOrderStatus.FAILED);
                throw e;
            }

        }

        LOG.info("Shipment processed successfully"
                + (successCounter == shipments.size()));
        if (successCounter == shipments.size()) {
            // All the shipments have successfully been processed and shipgen
            // request send. update the cache
            LOG.info("Marking the order as complete processing for GCFN");
            cpProvider.updateOrderStatus(response.getSequenceID(),
                    response.getExternalOrderID(), GCFNOrderStatus.COMPLETED);
        } else {
            String message = "There was a failure in sending one or more shipments for this order.Please check logs:"
                    + orderID;
            LOG.error(message);
        }

        LOG.debug("Exiting attemptShipgenForShipments() for orderId : "
                + orderID);
    }

    /**
     * Validates shipmentId, orderId and marketPlaceId Throws GCApplicationException incase of validation failure.
     *
     * @param shipmentId
     * @param orderId
     * @throws GCApplicationException
     */
    private void validateShipGenRequestInputs(String shipmentId,
                                              String orderId, long marketPlaceId) throws GCApplicationException {

        if (StringUtils.isEmpty(shipmentId)) {
            String msg = "shipmentId cannot be null or empty!";
            LOG.error(msg);
            throw new GCApplicationException(msg);
        }

        if (StringUtils.isEmpty(orderId)) {
            String msg = "orderId cannot be null or empty!";
            LOG.error(msg);
            throw new GCApplicationException(msg);
        }

        if (marketPlaceId == 0) {
            String msg = "marketPlaceId cannot be zero!";
            LOG.error(msg);
            throw new GCApplicationException(msg);
        }
    }

}
