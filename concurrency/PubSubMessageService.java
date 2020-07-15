package concurrency;

import com.amazon.kindle.BuildConstant;
import com.amazon.kindle.build.BuildInfo;
import com.amazon.kindle.krx.events.*;
import com.amazon.kindle.log.Log;
import com.amazon.kindle.util.NamedThreadFactory;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link IPubSubMessageService}
 *
 * @author donghao
 *
 */
public class PubSubMessageService extends BasePubSubEventsManager {
    private static final String TAG = Log.getTag(PubSubMessageService.class);
    public static final int DEFAULT_MIN_POOL_SIZE = 0;
    public static final int DEFAULT_MAX_POOL_SIZE = 2;
    public static final long DEFAULT_THREAD_IDLE_TIMEOUT = 60L; // seconds
    public static final long DEFAULT_THREAD_POOL_SHUTDOWN_TIMEOUT = 100L; // milliseconds
    private static final AtomicInteger executorNumber = new AtomicInteger(1);

    /**
     * a {@link WeakHashMap} that keeps the subscriber object to a list of
     * {@link EventHandler} mapping.
     */
    Map<Class, Collection<EventHandler>> handlersMap = new ConcurrentHashMap<>();

    Map<Class, Collection<TopicEventHandler>> topicHandlersMap = new ConcurrentHashMap<>();

    /**
     * Mapping of {@link IEvent} class types to a collection of
     * {@link EventHandler} that will be invoked when such event is published.
     */
    private Map<Class<?>, Collection<EventHandler>> eventTypeToHandlers = new ConcurrentHashMap<>();


    private Map<String, Collection<TopicEventHandler>> topicToHandlers = new ConcurrentHashMap<>();

    /**
     * Mapping of publisher class type to {@link IMessageQueue} instance.
     */
    private Map<Class<?>, IMessageQueue> messageQueues = new ConcurrentHashMap<>();

    /**
     * Mapping of topic to {@link ITopicMessageQueue} instance.
     */
    private Map<String, ITopicMessageQueue> topicToQueue = new ConcurrentHashMap<>();

    /**
     * instance of {@link DeadEventHandler} to handle any message that is not
     * handled.
     */
    private EventHandler deadMessageHandler = new DeadEventHandler();

    private IMessageQueue messageQueue = new MessageQueue(this, deadMessageHandler);

    private volatile static PubSubMessageService instance = null;

    private static String subscriber_finder_class = "com.amazon.kindle.services.events.SubscriberFinder";

    private static ISubscriberFinder finder = null;

    private static ExecutorService eventExecutor = null;

    private boolean centralExecutor = false;

    //This is to avoid concurrent issue for subscribe and unsubscribe
    private Object subscribeLock = new Object();

    private static void init() {
        try {
            Class<?> clazz = Class.forName(subscriber_finder_class);

            finder = (ISubscriberFinder) clazz.newInstance();
            Log.info(TAG, "PubSub scanned all the subscribers");
        } catch (Exception ex) {
            Log.error(TAG, subscriber_finder_class + " not found");
        }
    }

    /**
     * get an instance of {@link IPubSubEventsManager}
     *
     * @return
     */
    public static IPubSubEventsManager getInstance() {
        if (instance == null) {
            synchronized (IPubSubEventsManager.class) {
                if (instance == null) {
                    init();
                    instance = new PubSubMessageService();
                }
            }
        }
        return instance;
    }

    /**
     * testing only: create a {@link IPubSubEventsManager} implementation using
     * a custom {@link ISubscriberFinder} implementation
     *
     * @param finderClassName
     * @return
     */
    static synchronized IPubSubEventsManager getInstance(String finderClassName) {
        subscriber_finder_class = finderClassName;
        instance = null;
        return getInstance();
    }

    PubSubMessageService() {
        this.centralExecutor = BuildInfo.isEInkBuild();
        if (this.centralExecutor) {
            eventExecutor = newExecutor();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazon.kindle.krx.events.IPubSubEventsManager#createMessageQueue(
     * java.lang.Class)
     */
    @Override
    public IMessageQueue createMessageQueue(Class<?> publisher) {
        if (centralExecutor) {
            return messageQueue;
        }
        return createMessageQueue(publisher, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazon.kindle.krx.events.IPubSubEventsManager#createMessageQueue(
     * java.lang.Class, java.util.concurrent.ExecutorService)
     */
    @Override
    @VisibleForTesting
    public synchronized IMessageQueue createMessageQueue(Class<?> publisher,
                                                         ExecutorService executor) {
        IMessageQueue queue = messageQueues.get(publisher);
        if (queue == null) {
            queue = new MessageQueue(this, executor,
                    deadMessageHandler);
            messageQueues.put(publisher, queue);
        }

        return queue;
    }

    @Override
    public ITopicMessageQueue createTopicMessageQueue(String topic) {
        ITopicMessageQueue queue = topicToQueue.get(topic);
        if (queue == null) {
            queue = new TopicMessageQueue(topic, this, null, deadMessageHandler);
            topicToQueue.put(topic, queue);
        }

        return queue;
    }

    /**
     * get all the handlers for a given {@link IEvent}
     *
     * @param event
     * @return
     */
    Collection<EventHandler> getHandlersForEvent(IEvent event) {
        return eventTypeToHandlers.get(event.getClass());
    }

    /**
     * get all the handlers for a given {@link String} topic
     * @param topic
     * @return
     */
    Collection<TopicEventHandler> getHandlersForTopic(String topic) {
        return topicToHandlers.get(topic);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazon.kindle.krx.events.IPubSubEventsManager#subscribe(java.lang
     * .Object)
     */
    @Override
    public void subscribe(Object listener) {
        synchronized (subscribeLock) {
            Class clazz = listener.getClass();

            // get all the handlers that this listener subscribe to
            Collection<EventHandler> handlers = null;
            Collection<TopicEventHandler> topicHandlers = null;

            handlers = handlersMap.get(clazz);

            if (handlers == null) {
                if (finder != null) {
                    handlers = finder.findSubscribers(listener);
                } else {
                    handlers = EventHandler.createEventHandlers(listener);
                }
            }
            RegisterEventHandlers(listener, handlers);

            topicHandlers = topicHandlersMap.get(clazz);
            if (topicHandlers == null) {
                if (finder != null) {
                    topicHandlers = finder.findTopicSubscribers(listener);
                } else {
                    topicHandlers = TopicEventHandler.createEventHandlers(listener);
                }
            }
            RegisterTopicHandlers(listener, topicHandlers);
        }
    }

    private void RegisterTopicHandlers(Object listener, Collection<TopicEventHandler> handlers) {
        if (handlers == null || handlers.isEmpty()) {
            if (!BuildConstant.OFFICIAL && Log.isDebugLogEnabled()) {
                Log.debug(TAG, "No topic handlers to register for listener " + listener.getClass());
            }
            return;
        }

        Class clazz = listener.getClass();
        boolean inited = topicHandlersMap.containsKey(clazz);

        if (!inited) {
            topicHandlersMap.put(clazz, handlers);
        }

        for (TopicEventHandler handler : handlers) {
            if (!inited) {
                Collection<TopicEventHandler> handlersList = topicToHandlers.get(handler.topic);
                if (handlersList == null) {
                    handlersList = new CopyOnWriteArrayList<TopicEventHandler>();
                    topicToHandlers.put(handler.topic, handlersList);
                }
                handlersList.add(handler);
            }
            handler.registerListener(listener);
        }
    }

    private void RegisterEventHandlers(Object listener, Collection<EventHandler> handlers) {
        // do nothing if no handler was found in the listener
        if (handlers == null || handlers.isEmpty()) {
            return;
        }

        Class clazz = listener.getClass();
        boolean inited = handlersMap.containsKey(clazz);

        // register all the handlers from this listener
        if (!inited) {
            handlersMap.put(clazz, handlers);
        }

        for (EventHandler handler : handlers) {
            if (!inited) {
                Collection<EventHandler> handlersList = eventTypeToHandlers
                        .get(handler.getEventType());
                if (handlersList == null) {
                    handlersList = new CopyOnWriteArrayList<EventHandler>();
                    eventTypeToHandlers
                            .put(handler.getEventType(), handlersList);
                }
                handlersList.add(handler);
            }
            handler.registerListener(listener);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazon.kindle.krx.events.IPubSubEventsManager#unsubscribe(java.lang
     * .Object)
     */
    @Override
    public void unsubscribe(Object listener) {
        synchronized (subscribeLock) {
            Class clazz = listener.getClass();

            // remove from the listener -> event handlers map
            Collection<EventHandler> handlers = handlersMap.get(clazz);
            if (handlers != null) {
                // now remove all event handlers declared in this listener from the
                // eventType -> handlers map.
                for (EventHandler handler : handlers) {
                    handler.unregisterListener(listener);
                }
            }

            // remove from the listener -> topic handlers map
            Collection<TopicEventHandler> topicHandlers = topicHandlersMap.get(clazz);
            if (topicHandlers != null) {
                // now remove all event handlers declared in this listener from the
                // topic -> handlers map.
                for (TopicEventHandler topicHandler : topicHandlers) {
                    topicHandler.unregisterListener(listener);
                }
            }
        }
    }

    /**
     * remove the handler if the target object (the subscriber) has been
     * de-referenced (gc-ed)
     *
     * @param handler
     */
    void removeHandler(BaseEventHandler handler) {
        if (!handler.hasListener()) {
            if (handler instanceof EventHandler) {
                EventHandler typeEventHandler = (EventHandler) handler;
                Class<?> eventType = typeEventHandler.getEventType();
                Collection<EventHandler> handlers = eventTypeToHandlers.get(eventType);
                if (handlers != null) {
                    handlers.remove(handler);
                    if (handlers.isEmpty()) {
                        eventTypeToHandlers.remove(eventType);
                    }
                }
            } else if (handler instanceof TopicEventHandler) {
                TopicEventHandler topicEventHandler = (TopicEventHandler) handler;
                String topic = topicEventHandler.topic;
                Collection<TopicEventHandler> handlers = topicToHandlers.get(topic);
                if (handlers != null) {
                    handlers.remove(handler);
                    if (handlers.isEmpty()) {
                        topicToHandlers.remove(topic);
                    }
                }
            }
        }
    }

    ExecutorService getMessageQueueExecutor() {
        if (centralExecutor) {
            return eventExecutor;
        } else {
            return newExecutor();
        }
    }

    @Nonnull
    private static ExecutorService newExecutor() {
        return new ThreadPoolExecutor(DEFAULT_MIN_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE, DEFAULT_THREAD_IDLE_TIMEOUT,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(),
                new NamedThreadFactory("pubsub-" + executorNumber.getAndIncrement()));
    }

    void shutdownExecutor(ExecutorService executor) {
        if (executor == null || executor == eventExecutor) {
            return;
        } else {
            executor.shutdown();
            try {
                // wait for 100ms and then force shutdown the executor
                if (!executor.awaitTermination(
                        DEFAULT_THREAD_POOL_SHUTDOWN_TIMEOUT,
                        TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Log.warn(TAG, "Error shutting down executor: " + e.getMessage());
            }
        }
    }
}