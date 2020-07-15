package concurrency;

import com.amazon.aes.cm.exceptions.sql.NoRowsReturnedException;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to lazy-load and store the plethora of droplet-classes and droplet-class-values in CMDB.
 * To use this class, you should create a new instance of it with the appropriate thread-local DbHelper.
 * You can then call the utility methods, and they will cache any results in a series of static maps.
 * @author dekadt
 */
public class DropletClassMapper {
    private static Map<Integer, String> dropletClassIdNameMap = new ConcurrentHashMap<Integer, String>();
    private static Map<String, Integer> dropletClassNameIdMap = new ConcurrentHashMap<String, Integer>();
    private static Map<Integer, Map<String, Integer>> dropletClassValueNameIdMaps = new ConcurrentHashMap<Integer, Map<String,Integer>>();
    private static Map<Integer, Map<Integer, String>> dropletClassValueIdNameMaps = new ConcurrentHashMap<Integer, Map<Integer,String>>();

    private DbHelperProvider dbHelperProvider;

        public DropletClassMapper(DbHelperProvider dbHelperProvider) {
        this.dbHelperProvider = dbHelperProvider;
    }

    private DropletDbHelper getDropletDbHelper() {
        return dbHelperProvider.getDropletDbHelper();
    }

    /**
     * Returns the name of a droplet class from its ID.
     * @param dropletClassId The ID in question.
     * @return The name of the droplet class.
     * @throws DropletClassMapperException If something went wrong.
     * @throws UnknownDropletClassException If the droplet class is unknown.
     */
    public String getDropletClassName(int dropletClassId) throws DropletClassMapperException, UnknownDropletClassException {
        String name = dropletClassIdNameMap.get(dropletClassId);
        if (name == null) {
            name = getDropletClassNameFromDataSource(dropletClassId);
            if (name != null) {
                dropletClassIdNameMap.put(dropletClassId, name);
                dropletClassNameIdMap.put(name, dropletClassId);
            }
        }
        return name;
    }

    /**
     * Retries the name of the droplet class for a given ID from the data source. This bypasses any caches. Can be overridden for testing.
     * @param dropletClassId The droplet class ID in question.
     * @return The name of the droplet class in question.
     * @throws UnknownDropletClassException If the droplet class is unknown.
     * @throws DropletClassMapperException If something went wrong retrieving the mapping.
     */
    protected String getDropletClassNameFromDataSource(int dropletClassId) throws UnknownDropletClassException, DropletClassMapperException {
        try {
            return getDropletDbHelper().getDropletClassNameFromId(dropletClassId);
        } catch (NoRowsReturnedException e) {
            throw new UnknownDropletClassException("No droplet-class with ID '" + dropletClassId + "' was found.", e);
        } catch (Exception e) {
            throw new DropletClassMapperException("Error occurred finding a mapping for droplet-class with ID '" + dropletClassId + "'.", e);
        }
    }

    /**
     * Retries a droplet class ID from a droplet class name.
     * @param dropletClassName The name of the droplet class in question.
     * @return The ID of the droplet class.
     * @throws DropletClassMapperException If something went wrong retrieving the mapping.
     * @throws UnknownDropletClassException If the droplet class is unknown.
     */
    public int getDropletClassId(String dropletClassName) throws DropletClassMapperException, UnknownDropletClassException {
        Integer id = dropletClassNameIdMap.get(dropletClassName);
        if (id == null) {
            id = getDropletClassIdFromDataSource(dropletClassName);
            dropletClassNameIdMap.put(dropletClassName, id);
            if (id != null) {
                dropletClassIdNameMap.put(id, dropletClassName);
            }
        }
        return id;
    }

    /**
     * Method to get the mapping from the data source, bypassing the cache.
     * Can be overridden for test purposes.
     * @param dropletClassName
     * @return The integral ID for the name.
     * @throws SQLException
     */
    protected int getDropletClassIdFromDataSource(String dropletClassName) throws DropletClassMapperException, UnknownDropletClassException {
        try {
            return getDropletDbHelper().getDropletClassIdFromName(dropletClassName);
        } catch (NoRowsReturnedException e) {
            throw new UnknownDropletClassException("No droplet-class with name '" + dropletClassName + "' was found.", e);
        } catch (Exception e) {
            throw new DropletClassMapperException("Error occurred finding a mapping for droplet-class with name '" + dropletClassName + "'.", e);
        }
    }

    /**
     * Gets the the droplet class value name for a given droplet class ID and droplet class value ID
     * @param dropletClassId The droplet class ID.
     * @param dropletClassValueId The droplet class value ID.
     * @return The name of the droplet class value.
     * @throws DropletClassMapperException If something went wrong retrieving the mapping.
     * @throws UnknownDropletClassValueException If the droplet class value is unknown for the given droplet class.
     */
    public String getDropletClassValueName(int dropletClassId, int dropletClassValueId) throws DropletClassMapperException, UnknownDropletClassValueException {
        /* First just get the mapping for the droplet-class-id to ensure that it is valid */
        getDropletClassName(dropletClassId);

        /* Get the two maps we will need for this */
        Map<Integer, String> dcvIdNameMap = dropletClassValueIdNameMaps.get(dropletClassId);
        Map<String, Integer> dcvNameIdMap = dropletClassValueNameIdMaps.get(dropletClassId);

        /* If either is null, make a new one and put it into the super-map */
        if (dcvIdNameMap == null) {
            dcvIdNameMap = new ConcurrentHashMap<Integer, String>();
            dropletClassValueIdNameMaps.put(dropletClassId, dcvIdNameMap);
        }

        if (dcvNameIdMap == null) {
            dcvNameIdMap = new ConcurrentHashMap<String, Integer>();
            dropletClassValueNameIdMaps.put(dropletClassId, dcvNameIdMap);
        }

        String name = dcvIdNameMap.get(dropletClassValueId);

        /* If we don't have a mapping, get it from the DB, and update the in-memory mapping */
        if (name == null) {
            name = getDropletClassValueNameFromDataSource(dropletClassId,
                    dropletClassValueId);
            dcvIdNameMap.put(dropletClassValueId, name);
            if (name != null) {
                dcvNameIdMap.put(name, dropletClassValueId);
            }
        }

        return name;
    }

    /**
     * Retrieves the name of a droplet class value for a given droplet class ID and droplet class value ID from the data source.
     * @param dropletClassId The droplet class ID in question.
     * @param dropletClassValueId The droplet class value in question.
     * @return The name of the droplet class value.
     * @throws UnknownDropletClassValueException If the droplet class value is unknown.
     * @throws DropletClassMapperException If something went wrong retrieving the mapping.
     */
    private String getDropletClassValueNameFromDataSource(int dropletClassId, int dropletClassValueId) throws UnknownDropletClassValueException,
            DropletClassMapperException {
        try {
            return getDropletDbHelper().getDropletClassValueNameFromId(dropletClassId, dropletClassValueId);
        } catch (NoRowsReturnedException e) {
            throw new UnknownDropletClassValueException("No droplet-class-value with id '" + dropletClassValueId +
                    "'for droplet-class-id '" + dropletClassId + "' was found.", e);
        } catch (Exception e) {
            throw new DropletClassMapperException("Error occurred retrieving mapping for droplet-class-value with id '" + dropletClassValueId +
                    "' and droplet-class-id '" + dropletClassId + "' was found.", e);
        }
    }

    /**
     * As above, but uses droplet class name instead of ID. Simply wraps the above method.
     * @param dropletClassName
     * @param dropletClassValueId
     * @return
     * @throws DropletClassMapperException
     */
    public String getDropletClassValueName(String dropletClassName, int dropletClassValueId) throws DropletClassMapperException {
        return getDropletClassValueName(getDropletClassId(dropletClassName), dropletClassValueId);
    }

    /**
     * Exception to indicate a problem has occured during the mapping of droplet classes/droplet class values.
     */
    public class DropletClassMapperException extends Exception {
        private static final long serialVersionUID = 921287534156364816L;

        public DropletClassMapperException(String message) {
            super(message);
        }

        public DropletClassMapperException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception to indicate that the requested droplet class is unknown.
     */
    public class UnknownDropletClassException extends DropletClassMapperException {
        private static final long serialVersionUID = 921287784156364816L;

        public UnknownDropletClassException(String message) {
            super(message);
        }

        public UnknownDropletClassException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An Exception to indicate that the requested droplet class value is unknown
     */
    public class UnknownDropletClassValueException extends DropletClassMapperException {
        private static final long serialVersionUID = 911287784156364814L;

        public UnknownDropletClassValueException(String message) {
            super(message);
        }

        public UnknownDropletClassValueException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}