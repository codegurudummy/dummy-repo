package concurrency;

import amazon.cj.crypto.CryptoException;
import amazon.cj.dbaccess.pwstore.impl.OdinPasswordStore;
import amazon.fc.config.AppConfig;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;
import com.amazon.fc.transshipment.persistence.MultiWarehouseContextHolder;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

@Slf4j
@Component
public class PostgresMultiWarehouseDataSource extends AbstractDataSource
        implements ApplicationListener<ContextRefreshedEvent>
{
    private ConcurrentHashMap<String, DataSource> dataSourceConnectionCache = new ConcurrentHashMap<>();

    private boolean contextInitialized = false;

    private static String jdbcUrlTemplate = AppConfig.findString("PostgreSQL.jdbcUrlTemplate");

    private static String odinMaterialSetTemplate = AppConfig.findString("PostgreSQL.odinMaterialSetTemplate");

    /* See: https://jdbc.postgresql.org/documentation/head/connect.html for other property names / values */
    private static final String PROPERTY_NAME_USERNAME = "user";
    private static final String PROPERTY_NAME_PASSWORD = "password";
    private static final String PROPERTY_NAME_SSL_ENABLED = "ssl";
    private static final String PROPERTY_VALUE_SSL_ENABLED = "true";
    private static final String PROPERTY_NAME_SSL_MODE = "sslmode";
    private static final String PROPERTY_VALUE_SSL_MODE = "verify-ca"; // TODO: Change to verify-full before prod rollout.
    private static final String PROPERTY_NAME_SSL_CERT_PATH = "sslrootcert";
    private static final String PROPERTY_VALUE_SSL_CERT_PATH = AppConfig.findString("PostgreSQL.certPath");
    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String PROPERTY_NAME_APPLICATION_NAME = "ApplicationName";
    private static final String PROPERTY_VALUE_APPLICATION_NAME = "AFTTransshipmentManifestService";

    private DataSource resolveTargetDataSource() throws CryptoException, PropertyVetoException
    {
        String warehouseId = MultiWarehouseContextHolder.getFcEnvironment().getWarehouseId();
        log.debug("resolveTargetDataSource for warehouseId: {}", warehouseId);

        if(warehouseId == null)
        {
            if(contextInitialized)
            {
                String errorMessage = "WarehouseId is null and application is running, it must be bound in warehouseIdManager";
                log.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            log.info("Creating empty postgres datasource for application initialization.");
            return new DriverManagerDataSource();
        }
        return getPostgresDataSourceForWarehouseId(warehouseId);
    }

    private DataSource getPostgresDataSourceForWarehouseId(String warehouseId) throws CryptoException, PropertyVetoException
    {
        synchronized (this)
        {
            if (dataSourceConnectionCache.containsKey(warehouseId))
            {
                log.debug("Found a cached DataSource for warehouseId: {}", warehouseId);
                return dataSourceConnectionCache.get(warehouseId);
            }
            else
            {
                log.info("Creating new DataSource for warehouseId: {}", warehouseId);

                OdinAWSCredentialsProvider access = new OdinAWSCredentialsProvider(buildOdinMaterialSetName(warehouseId));

                Properties props = new Properties();
                props.setProperty(PROPERTY_NAME_USERNAME, access.getCredentials().getAWSAccessKeyId());
                props.setProperty(PROPERTY_NAME_PASSWORD, decryptOdinPassword(access.getCredentials().getAWSSecretKey()));
                props.setProperty(PROPERTY_NAME_SSL_ENABLED, PROPERTY_VALUE_SSL_ENABLED);
                props.setProperty(PROPERTY_NAME_SSL_MODE, PROPERTY_VALUE_SSL_MODE);
                props.setProperty(PROPERTY_NAME_SSL_CERT_PATH, PROPERTY_VALUE_SSL_CERT_PATH);
                props.setProperty(PROPERTY_NAME_APPLICATION_NAME, PROPERTY_VALUE_APPLICATION_NAME);

                String connectionUrl = buildJdbcUrl(warehouseId);
                ComboPooledDataSource dataSource = new ComboPooledDataSource();
                dataSource.setDriverClass(POSTGRES_DRIVER_CLASS);
                dataSource.setProperties(props);
                dataSource.setJdbcUrl(connectionUrl);

                dataSourceConnectionCache.put(warehouseId, dataSource);
                log.info("Successfully created new Postgres DataSource for warehouseId: {} and placed into local cache", warehouseId);
                return dataSource;
            }
        }
    }

    private String decryptOdinPassword(String encryptedPassword) throws CryptoException
    {
        OdinPasswordStore odinPasswordStore = new OdinPasswordStore();
        String decryptedPassword = odinPasswordStore.decryptPassword(encryptedPassword);
        log.debug("Successfully decrypted password");
        return decryptedPassword;
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        DataSource dataSource = null;
        try
        {
            dataSource = resolveTargetDataSource();
        }
        catch (CryptoException | PropertyVetoException e)
        {
            throw new SQLException(e.getMessage());
        }

        return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException
    {
        DataSource dataSource = null;
        try
        {
            dataSource = resolveTargetDataSource();
        }
        catch (CryptoException | PropertyVetoException e)
        {
            throw new SQLException(e.getMessage());
        }

        return dataSource.getConnection(username, password);
    }

    private String buildJdbcUrl(String warehouseId)
    {
        String url;
        try
        {
            // Look for an FC specific jdbc url template
            url = AppConfig.findString(String.format("%s.PostgreSQL.jdbcUrlTemplate", warehouseId.toUpperCase()));
        }
        catch (Exception e)
        {
            // couldn't find fc specific jdbc url template, use default postgres jdbc url template structure.
            url = getDefaultJdbcUrlStructure(warehouseId);
        }

        if(url == null)
        {
            // couldn't find fc specific jdbc url template, use default postgres jdbc url template structure.
            url = getDefaultJdbcUrlStructure(warehouseId);
        }

        log.info("Built warehouseId: {}'s JDBC URL: {}", warehouseId, url);
        return url;
    }

    private String buildOdinMaterialSetName(String warehouseId)
    {
        String odinMaterialSetName;
        try
        {
            // Look for an FC specific odin set
            odinMaterialSetName = AppConfig.findString(String.format("%s.PostgreSQL.odinMaterialSetTemplate", warehouseId.toUpperCase()));
        }
        catch (Exception e)
        {
            // couldn't find fc specific odin, use default postgres odin set naming structure.
            odinMaterialSetName = getDefaultOdinMaterialSetNameStructure(warehouseId);
        }

        if(odinMaterialSetName == null)
        {
            // couldn't find fc specific odin, use default postgres odin set naming structure.
            odinMaterialSetName = getDefaultOdinMaterialSetNameStructure(warehouseId);
        }

        log.info("Built warehouseId: {}'s odinSetName: {}", warehouseId, odinMaterialSetName);
        return odinMaterialSetName;
    }

    public void logCurrentPostgresWarehouseIds()
    {
        log.info("There are currently {} total warehouseIds with a cached POSTGRES datasource: {}", dataSourceConnectionCache.size(), dataSourceConnectionCache.keySet());
    }

    private String getDefaultJdbcUrlStructure(String warehouseId)
    {
        return jdbcUrlTemplate.replaceAll("%FC%", warehouseId.toLowerCase());
    }

    private String getDefaultOdinMaterialSetNameStructure(String warehouseId)
    {
        return odinMaterialSetTemplate.replaceAll("%FC%", warehouseId.toLowerCase());
    }

    /*
    *   ContextRefreshEvent occurs when an ApplicationContext gets initialized or refreshed.
    *   This will set boolean to true once all other beans have been initialized.
    */
    public void onApplicationEvent(ContextRefreshedEvent event)
    {
        contextInitialized = true;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
}