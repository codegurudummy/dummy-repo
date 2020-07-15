package concurrency;

import amazon.DeviceMaster.service.InvalidArgumentException;
import amazon.DeviceMaster.service.util.GuiceConfig;
import amazon.fiona.util.SuperLogger;
import amazon.platform.config.AppConfig;
import amazon.platform.profiler.Profiler;
import amazon.platform.profiler.ProfilerScope;
import com.amazon.DMS.Localization.LocalizationHelper;

/**
 * Concrete class for TemplateNameProvider interface. Populates the deviceName and defaultDeviceName
 * from worldStringDB  
 * @author sushils
 *
 */

public class WorldStringDBNameProvider implements TemplateNameProviderInterface
{
    private static final SuperLogger log = SuperLogger.getLogger(WorldStringDBNameProvider.class);
    private static String DEFAULT_DEVICE_TYPE = "kindle";
    private String m_deviceName;
    private String m_defaultDeviceName;

    private String m_deviceType;
    private String m_language;
    private String m_defaultDeviceType;

    static LocalizationHelper m_localizationHelper = 
            GuiceConfig.getInjector().getInstance(LocalizationHelper.class);

    public WorldStringDBNameProvider(final String deviceType, final String language)
    {
        if(deviceType != null && deviceType.trim().length() >= 0) {
            m_deviceType = deviceType;
        } else {
            m_deviceType = DEFAULT_DEVICE_TYPE;
        }

        if(language != null && language.trim().length() >=0 ) {
            m_language = language;
        } else {
            m_language = "en-US";
        }
    }

    public void setDefaultDeviceType(final String defaultDeviceType)
    {
        if (defaultDeviceType != null && defaultDeviceType.trim().length() >= 0) {
            m_defaultDeviceType = defaultDeviceType;
        } else {
            m_defaultDeviceType = DEFAULT_DEVICE_TYPE;
        }
    }

    @Override
    public String getDeviceName() throws InvalidArgumentException
    {
        if(m_deviceName != null) {
            return m_deviceName;
        }
        final String deviceNameTemplate = AppConfig.findString("DeviceNameTemplate");
        m_deviceName = getLocalizedTemplate(deviceNameTemplate, m_language);
        return m_deviceName; 
    }

    @Override
    public String getDefaultDeviceName() throws InvalidArgumentException
    {
        if(m_defaultDeviceName != null) {
            return m_defaultDeviceName;
        }
        final String defaultDeviceNameTemplate = AppConfig.findString("DefaultDeviceNameTemplate");
        m_defaultDeviceName = getLocalizedTemplate(defaultDeviceNameTemplate, m_language);
        return m_defaultDeviceName; 
    }


    /**
     * Query the worldstringDB for a given template, deviceType and language.
     * If the query results a null then query for template, default device type and language
     * If it still returns a null then throw the exception
     * @param template
     * @param language
     * @return Localized string
     */
    private String getLocalizedTemplate(final String template, String language)
    throws InvalidArgumentException
    {
        final ProfilerScope scope = Profiler.scopeStart("WorldStringDBNameProvider.GetLocalizedTemplate");
        String locale = m_localizationHelper.getLocaleForErrorMessages(language);
        
        String localizedStringID = template + "-" + m_deviceType;
        String resultTemplate = getLocalizedTemplateforLocale(locale, localizedStringID);
        
        if(resultTemplate != null) {
            Profiler.scopeEnd(scope);
            return resultTemplate;
        }

        if (m_defaultDeviceType != null) {
            // if default device type is explicitly configured then use it;
            // otherwise, we will use 'kindle' as default device type.
            localizedStringID = template + "-" + m_defaultDeviceType;
        } else {
            localizedStringID = template + "-" + DEFAULT_DEVICE_TYPE;
        }
        resultTemplate = getLocalizedTemplateforLocale(locale, localizedStringID);
        
        if(resultTemplate != null) {
            Profiler.scopeEnd(scope);
            return resultTemplate;
        }
        ProfilerScope.addCounter("WorldStringDBNameProvider.TemplateNotFound", 1, "count");
        Profiler.scopeEnd(scope);
        return resultTemplate;
    }
    
    private String getLocalizedTemplateforLocale(String locale , String localizedStringID)
    {
        String resultTemplate = null;
        try {
            resultTemplate = m_localizationHelper.getWorldServerStringForLocale(localizedStringID, locale);
        } catch (Exception e) {
            log.info("Template " + localizedStringID + " is not present in world string db for locale " + locale);
            resultTemplate = null;  /* saying is explicitly */
        }
        
        return resultTemplate;
    }

}