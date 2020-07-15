/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package concurrency;

import com.amazon.correios.notification.component.runtime.ComponentContext;
import com.amazon.traffic.cafe.component.annotator.strategy.AnnotationStrategy;
import com.amazon.traffic.cafe.component.annotator.strategy.AnnotationStrategyFactory;
import com.amazon.traffic.cafe.component.annotator.strategy.AnnotationStrategyType;
import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Utility class to fetch AnnotationStrategy object for the given annotationStrategy type,
 * marketplaceId. It uses concurrent HashMap to hold the instance of RODBAccess.
 * This ensures that we don't create the multiple instance of RODBAccess for same strategy type.
 */
@UtilityClass
public class AnnotationStrategyHelper {

    /***
     * map to hold the instance of AnnotationStrategy. This ensures that we don't create
     * the multiple instance of RODBAccess for same strategy type.
     */
    private Map<String, AnnotationStrategy> annotationStrategyMap =
            new ConcurrentHashMap<String, AnnotationStrategy>();

    /**
     * Annotator strategy key format to be added in the concurrent hashmap for
     * annotation strategies. It is a combination of strategy type and
     * marketplace.
     */
    private static final String ANNOTATOR_STRATEGY_KEY_FORMAT = "%s-%s";

    /**
     * Get AnnotationStrategy for the given annotation strategy type and marketplace.
     *
     * @param annotationStrategy
     *              annotation strategy type
     * @param marketplaceId
     *              marketplace id
     * @param context
     *              ComponentContext object
     * @return AnnotationStrategy object
     */
    public AnnotationStrategy getAnnotationStrategy(final String annotationStrategy,
                                                    final String marketplaceId,
                                                    final ComponentContext context) {

        AnnotationStrategyType annotationStrategyType = AnnotationStrategyType.valueOf(annotationStrategy);
        String annotatorKey = String.format(ANNOTATOR_STRATEGY_KEY_FORMAT, annotationStrategy, marketplaceId);
        AnnotationStrategy annotator = annotationStrategyMap.get(annotatorKey);

        if (annotator == null) {
            synchronized (AnnotationStrategyHelper.class) {
                annotator = annotationStrategyMap.get(annotatorKey);
                if (annotator == null) {
                    annotator = AnnotationStrategyFactory.getFilterStrategy(annotationStrategyType, marketplaceId,
                            context);
                    annotationStrategyMap.put(annotatorKey, annotator);
                }
            }
        }

        return annotator;
    }
}