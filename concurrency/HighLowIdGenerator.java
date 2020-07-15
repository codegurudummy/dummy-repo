/**
 * Copyright (c) 2011 Amazon.com Inc. All Rights Reserved.
 * AMAZON.COM CONFIDENTIAL
 * 
 * $Id: //brazil/src/appgroup/webservices/libs/AWSBillManagerDomain/mainline/src/amazon/webservices/platform/billmanager/domain/id/HighLowIdGenerator.java#4 $
 * $Change: 5964435 $
 * $DateTime: 2012/08/07 22:45:28 $
 * $Author: sunilga $
 */

package concurrency;

import amazon.platform.config.AppConfig;
import amazon.platform.config.AppConfigException;
import amazon.webservices.platform.billmanager.domain.factories.FactoryConfigurator;
import amazon.webservices.platform.billmanager.domain.model.IdSequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to generate unique identifiers using high and low values. Thread safe.
 */
public class HighLowIdGenerator implements IdGenerator {
    /**
     * Class to represent high and low values shared between threads
     */
    private static class SharedHighLowValues {
        public final long highValue;
        public AtomicLong nextLowValue;
        
        /**
         * @param highValue high value
         * @param nextLowValue next low value
         */
        public SharedHighLowValues(long highValue, long nextLowValue) {
            this.highValue = highValue;
            this.nextLowValue = new AtomicLong(nextLowValue);
        }
    }
    
    /**
     * Class to represent high and low values for id generation
     */
    private static class HighLowValues {
        public final long highValue;
        public final long lowValue;
        
        /**
         * @param highValue
         * @param lowValue
         */
        public HighLowValues(long highValue, long lowValue) {
            this.highValue = highValue;
            this.lowValue = lowValue;
        }
    }
    
    /**
     * Logger for this class.
     */
    private static final Log log = LogFactory.getLog(HighLowIdGenerator.class);
    
    /**
     * How many low values for each high value
     */
    private static final long ALLOCATION_SIZE = AppConfig
            .findInteger("AWSBillManagerDomain.HighLowIdGenerator.AllocationSize");
    
    private static final int LOCKING_RETRY_LIMIT = AppConfig
            .findInteger("AWSBillManagerDomain.HighLowIdGenerator.LockingRetryLimit", 20);
    
    /**
     * Map for high/low values using sequenceName as key.
     */
    private final ConcurrentHashMap<String, SharedHighLowValues> valuesForSequence = new ConcurrentHashMap<String, SharedHighLowValues>();
    private final ConcurrentHashMap<String, Object> locksForSequence = new ConcurrentHashMap<String, Object>();
    
    /**
     * @return How many low values for each high value
     */
    public long getAllocationSize() {
        return ALLOCATION_SIZE;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IllegalArgumentException
     * @throws AppConfigException
     * @throws HibernateException
     */
    @Override
    public long generateLongKey(String sequenceName) {
        if (ALLOCATION_SIZE < 1) {
            throw new AppConfigException(
                    "AllocationSize in config must be a positive integer.");
        } else if (sequenceName == null || sequenceName.isEmpty()) {
            throw new IllegalArgumentException("sequenceName null or empty.");
        }
        
        HighLowValues hiloValues = getNextHighLowValues(sequenceName);
        return hiloValues.highValue * ALLOCATION_SIZE + hiloValues.lowValue;
    }
    
    /**
     * Get a pair of valid high/low values.
     * 
     * @param sequenceName
     *            sequence name
     * @return high/low values
     */
    private HighLowValues getNextHighLowValues(String sequenceName) {
        SharedHighLowValues sharedHiloValues = getValuesForSequence(sequenceName);

        long lowValue = sharedHiloValues.nextLowValue.getAndIncrement();
        long highValue = sharedHiloValues.highValue;
        if (lowValue >= ALLOCATION_SIZE) {
            // No more low value available for current high value
            Object sequenceLock = getLockForSequence(sequenceName);
            synchronized (sequenceLock) {
                // Check if the previous thread in the block had already updated
                // the high/low values
                sharedHiloValues = valuesForSequence.get(sequenceName);
                lowValue = sharedHiloValues.nextLowValue.getAndIncrement();
                highValue = sharedHiloValues.highValue;
                if (lowValue >= ALLOCATION_SIZE) {
                    // Get high value from database.
                    // Only one thread will do this for each new high value.
                    highValue = getNextSequenceValue(sequenceName);
                    lowValue = 0;
                    valuesForSequence.put(sequenceName, new SharedHighLowValues(highValue, 1));
                }
            }
        }
        
        return new HighLowValues(highValue, lowValue);
    }

    /**
     * Get high/low values for sequence, insert dummy entry if absent.
     * 
     * @param sequenceName
     *            sequence name
     * @return shared high/low values
     */
    private SharedHighLowValues getValuesForSequence(String sequenceName) {
        SharedHighLowValues sharedHiloValues = valuesForSequence.get(sequenceName);
        if (sharedHiloValues == null) {
            SharedHighLowValues newSharedHiloValues = new SharedHighLowValues(
                    Long.MIN_VALUE, ALLOCATION_SIZE);
            sharedHiloValues = valuesForSequence.putIfAbsent(sequenceName, newSharedHiloValues);
            if (sharedHiloValues == null) {
                sharedHiloValues = newSharedHiloValues;
            }
        }
        
        return sharedHiloValues;
    }

    /**
     * Get lock for sequence.
     * 
     * @param sequenceName
     *            sequence name
     * @return lock for sequence
     */
    private Object getLockForSequence(String sequenceName) {
        Object sequenceLock = locksForSequence.get(sequenceName);
        if (sequenceLock == null) {
            Object newLock = new Object();
            sequenceLock = locksForSequence.putIfAbsent(sequenceName, newLock);
            if (sequenceLock == null) {
                sequenceLock = newLock;
            }
        }
        
        return sequenceLock;
    }
    
    /**
     * Get next sequence value from database as high value
     * 
     * @param sequenceName
     *            id sequence name
     * @return Next sequence value
     * @throws HibernateException
     */
    private long getNextSequenceValue(String sequenceName)
            throws HibernateException {
        StatelessSession session = FactoryConfigurator.getInstance().getObjectFactoryFactory().getHibernateSessionFactory().openStatelessSession();
        long nextValue = -1;
        
        // Optimistic locking
        for (int retry = 0; retry < LOCKING_RETRY_LIMIT && nextValue < 1; retry++) {
            Transaction transaction = session.beginTransaction();
            try {
                Criteria criteria = session.createCriteria(IdSequence.class)
                        .add(Restrictions.eq("sequenceName", sequenceName))
                        .setProjection(Projections.max("nextValue"));
                Long prevNextValue = (Long) criteria.uniqueResult();

                // Create new entry with nextValue = oldNextValue + 1
                IdSequence newIdSequence;
                if (prevNextValue == null) {
                    newIdSequence = createIdSequence(sequenceName, 2L);
                } else {
                    newIdSequence = createIdSequence(sequenceName, prevNextValue + 1);
                }

                // Try to insert
                try {
                    session.insert(newIdSequence);
                    transaction.commit();
                } catch (HibernateException e) {
                    // Failed, retry
                    log.debug("Optimistic locking failed, retry count: "
                            + retry + ", object: " + newIdSequence);
                    transaction.rollback();
                    continue;
                }

                // Succeed
                nextValue = newIdSequence.getNextValue() - 1;
            } catch (HibernateException e) {
                transaction.rollback();
                session.close();
                throw e;
            }
        }
        session.close();
        
        if (nextValue < 1) {
            throw new HibernateException("Cannot obtain lock for id sequences table.");
        }
        
        return nextValue;
    }
    
    /**
     * Create IdSequence object
     * 
     * @param sequenceName
     *            sequence name
     * @param nextHighValue
     *            next high value
     * @return new IdSequence object
     */
    private IdSequence createIdSequence(String sequenceName, Long nextHighValue) {
        IdSequence newIdSequence = new IdSequence();
        
        newIdSequence.setSequenceName(sequenceName);
        newIdSequence.setNextValue(nextHighValue);
        newIdSequence.setCreatedBy(AppConfig.getUser());
        newIdSequence.setCreatedByClient(AppConfig.getApplicationName());
        newIdSequence.setLastRequestId(AppConfig.getMachine());
        newIdSequence.setCreationDate(new Date());
        
        return newIdSequence;
    }
}