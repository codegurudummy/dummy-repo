package concurrency;

import com.booksurge.ocs.outsource.listeners.OutsourceStatusListener;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author henso
 */
@Service
public class BulkOutsourceStatusListenerContainer implements OutsourceStatusListener
{
  static final int MAX_REQUESTS_IN_QUEUE = 10;

  private final Queue<BulkOutsourceRequest> processedBulkOutsources = new LinkedList<>();

  public List<BulkOutsourceRequest> getProcessedBulkOutsourceRequests()
  {
    synchronized (processedBulkOutsources)
    {
      return new ArrayList<>(processedBulkOutsources);
    }
  }

  /**
   * Registers an outsource request with the listener.
   *
   * @param bulkOutsourceRequest bulk outsource request the outsource request to.
   * @param outsourceRequest outsource request to add to the container.
   */
  public void registerOutsourceRequest(BulkOutsourceRequest bulkOutsourceRequest, OutsourceRequest outsourceRequest)
  {
    if (bulkOutsourceRequest != null && outsourceRequest != null)
    {
      synchronized (processedBulkOutsources)
      {
        Set<BulkOutsourceRequest> processedBulkOutsourcesSet = new HashSet<>(processedBulkOutsources);

        if (!processedBulkOutsourcesSet.contains(bulkOutsourceRequest))
        {
          enforceQueueLimit();

          processedBulkOutsources.add(bulkOutsourceRequest);
        }

        bulkOutsourceRequest.addOutsourceRequest(outsourceRequest);
      }
    }
  }

  /**
   * Invoked when the status of an outsource request has changed on an order.
   *
   * @param orderId the order id associated with the outsource request.
   * @param outsourceStatus The new status
   */
  @Override
  public void onStatusChanged(String orderId, OutsourceStatus outsourceStatus)
  {
    OutsourceRequest request = findNewestOutsourceRequest(orderId);

    if (request != null)
    {
      request.setOutsourceStatus(outsourceStatus);
    }
  }

  /**
   * Invoked when the outsourced Order Id has been assigned by the outsourcing system.
   *
   * @param originalOrderId Original Order that was outsourced.
   * @param outsourcedOrderId The new order id that was injected at the other site.
   */
  @Override
  public void onOutsourcedOrderIdAssigned(String originalOrderId, String outsourcedOrderId)
  {
    OutsourceRequest request = findNewestOutsourceRequest(originalOrderId);

    if (request != null)
    {
      request.setOutsourcedOrderId(outsourcedOrderId);
    }
  }

  /**
   * Enforces Queue size. If the queue is already at max size, the queue is popped.
   */
  private void enforceQueueLimit()
  {
    synchronized (processedBulkOutsources)
    {
      if (processedBulkOutsources.size() == MAX_REQUESTS_IN_QUEUE)
      {
        processedBulkOutsources.remove();
      }
    }
  }

  /**
   * Finds newest outsource request matching order id
   *
   * @param orderId order id to search on
   *
   * @return OutsourceRequest if found, otherwise null.
   */
  private OutsourceRequest findNewestOutsourceRequest(String orderId)
  {
    OutsourceRequest toReturn = null;

    for (BulkOutsourceRequest bulkOutsourceRequest : processedBulkOutsources)
    {
      OutsourceRequest outsourceRequest = bulkOutsourceRequest.findOutsourceRequest(orderId);
      if (outsourceRequest != null)
      {
        toReturn = outsourceRequest;
      }
    }

    return toReturn;
  }
}