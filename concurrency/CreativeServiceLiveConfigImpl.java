package concurrency;

import com.a9.cpx.aax.common.StringTemplate;
import com.a9.cpx.aaxserver.liveconfig.AAXServerLiveConfig;
import com.a9.cpx.aaxserver.util.PMETUtil;
import com.a9.cpx.common.util.CallSiteHelper;
import com.a9.cpx.monitoring.indicators.Counter;
import com.a9.log.CommonLogger;
import org.apache.commons.lang.Validate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class CreativeServiceLiveConfigImpl implements CreativeService, InitializingBean {
    private static CommonLogger logger = CommonLogger.getLogger(CallSiteHelper.getCallingClassName());
    private AAXServerLiveConfig liveConfig;
    
    
    private ConcurrentHashMap<Long, Set<Creative>> localCache = new ConcurrentHashMap<Long, Set<Creative>>();
    private ConcurrentHashMap<Long, Map<String, Set<Creative>>> bidderCreativesBySize = new ConcurrentHashMap<Long, Map<String, Set<Creative>>>();
    private ConcurrentHashMap<Long, Map<String, Creative>> bidderCreativesById = new ConcurrentHashMap<Long, Map<String, Creative>>();

    private ConcurrentHashMap<Long, Set<CreativeTemplate>> bidderTemplates = new ConcurrentHashMap<Long, Set<CreativeTemplate>>();
    private ConcurrentHashMap<Long, Map<String, Set<CreativeTemplate>>> bidderTemplatesBySize = new ConcurrentHashMap<Long, Map<String, Set<CreativeTemplate>>>();
    private ConcurrentHashMap<Long, Map<String, CreativeTemplate>> bidderTemplatesById = new ConcurrentHashMap<Long, Map<String, CreativeTemplate>>();
    private ConcurrentHashMap<Long, ConcurrentHashMap<String, StringTemplate>> bidderCompiledTemplates = new ConcurrentHashMap<Long, ConcurrentHashMap<String, StringTemplate>>();
    
    private Counter aaxLiveConfigCreativeServiceErrorCounter;
    
    @Override
    public Set<Creative> getCreatives(long bidderId, String size) {
        Set<Creative> creatives = liveConfig.getCreativesForBidder(bidderId);
        if (CollectionUtils.isEmpty(creatives)) return Collections.emptySet();
        Set<Creative> cachedCreatives = localCache.get(bidderId);
        if (cachedCreatives == null || !creatives.equals(cachedCreatives)) { // if local cache is not same as live-config
            loadCreatives(bidderId, creatives);
        }
        
        return bidderCreativesBySize.get(bidderId).get(size);
    }

    @Override
    public Creative getCreative(long bidderId, String creativeId) {
        Set<Creative> creatives = liveConfig.getCreativesForBidder(bidderId);
        if (CollectionUtils.isEmpty(creatives)) return null;
        
        try {
            Set<Creative> cachedCreatives = localCache.get(bidderId);
            if (cachedCreatives == null || !creatives.equals(cachedCreatives)) { // if local cache is not same as live-config
                loadCreatives(bidderId, creatives);
            }
            
            return bidderCreativesById.get(bidderId).get(creativeId);
        } catch (Exception e) {
            PMETUtil.increment(aaxLiveConfigCreativeServiceErrorCounter);
            logger.warn("Error in getCreative; bidderId=%d, creativeId=%s", e, bidderId, creativeId);
        }
        
        return null;
    }
    
    void loadCreatives(long bidderId, Set<Creative> creatives) {
        Map<String, Set<Creative>> creativesPerSize = new HashMap<String, Set<Creative>>();
        Map<String, Creative> creativesById = new HashMap<String, Creative>();
        for (Creative creative : creatives) {
            Set<Creative> sizeCreatives = creativesPerSize.get(creative.getSize());
            if (sizeCreatives == null) {
                sizeCreatives = new HashSet<Creative>();
                creativesPerSize.put(creative.getSize(), sizeCreatives);
            }
            sizeCreatives.add(creative);
            
            creativesById.put(creative.getCreativeId(), creative);
        }
        bidderCreativesBySize.put(bidderId, creativesPerSize);
        bidderCreativesById.put(bidderId, creativesById);
        
    }

    @Override
    public Set<CreativeTemplate> getTemplates(long bidderId, String size) {
        Set<CreativeTemplate> templates = liveConfig.getCreativeTemplatesForBidder(bidderId);
        if (CollectionUtils.isEmpty(templates)) return Collections.emptySet();
        
        Set<CreativeTemplate> cachedTemplates = bidderTemplates.get(bidderId);
        if (cachedTemplates == null || !templates.equals(cachedTemplates)) {
            loadCreativeTemplates(bidderId, templates);
        }
        
        return bidderTemplatesBySize.get(bidderId).get(size);
    }
    
    void loadCreativeTemplates(long bidderId, Set<CreativeTemplate> templates) {
        Map<String, Set<CreativeTemplate>> templatesPerSize = new HashMap<String, Set<CreativeTemplate>>();
        Map<String, CreativeTemplate> templatesById = new HashMap<String, CreativeTemplate>();
        for (CreativeTemplate template : templates) {
            Set<CreativeTemplate> sizeTemplates = templatesPerSize.get(template.getSize());
            if (sizeTemplates == null) {
                sizeTemplates = new HashSet<CreativeTemplate>();
                templatesPerSize.put(template.getSize(), sizeTemplates);
            }
            sizeTemplates.add(template);
            templatesById.put(template.getTemplateId(), template);
        }
        
        bidderTemplatesBySize.put(bidderId, templatesPerSize);
        bidderTemplatesById.put(bidderId, templatesById);
        bidderCompiledTemplates.remove(bidderId);
    }

    @Override
    public CreativeTemplate getTemplate(long bidderId, String templateId) {
        Set<CreativeTemplate> templates = liveConfig.getCreativeTemplatesForBidder(bidderId);
        if (CollectionUtils.isEmpty(templates)) return null;
        
        try {
            Set<CreativeTemplate> cachedTemplates = bidderTemplates.get(bidderId);
            if (cachedTemplates == null || !templates.equals(cachedTemplates)) {
                loadCreativeTemplates(bidderId, templates);
            }
            
            return bidderTemplatesById.get(bidderId).get(templateId);
        } catch (Exception e) {
            PMETUtil.increment(aaxLiveConfigCreativeServiceErrorCounter);
            logger.warn("Error in getTemplate; bidderId=%d, templateId=%s", e, bidderId, templateId);
        }
        
        return null;
    }

    @Override
    public StringTemplate getCompiledTemplate(long bidderId, String templateId) {
        StringTemplate compiledTemplate;
        try {
            compiledTemplate = bidderCompiledTemplates.get(bidderId).get(templateId);
            if (compiledTemplate != null) return compiledTemplate;
        } catch (Exception e) {
            // ignore
        }
        
        CreativeTemplate creativeTemplate = getTemplate(bidderId, templateId);
        if (creativeTemplate == null) return null;
        
        try {
            compiledTemplate = new StringTemplate(creativeTemplate.getHtml());
            ConcurrentHashMap<String, StringTemplate> stringTemplates = bidderCompiledTemplates.get(bidderId);
            if (stringTemplates == null) {
                synchronized(bidderCompiledTemplates) {
                    stringTemplates = bidderCompiledTemplates.get(bidderId);
                    if (stringTemplates == null) {
                        stringTemplates = new ConcurrentHashMap<String, StringTemplate>();
                        bidderCompiledTemplates.put(bidderId, stringTemplates);
                    }
                }
            }
            stringTemplates.put(creativeTemplate.getTemplateId(), compiledTemplate);
            return compiledTemplate;
        } catch (Exception e) {
            PMETUtil.increment(aaxLiveConfigCreativeServiceErrorCounter);
            logger.warn("Error in getCompiledTemplate; bidderId=%d, templateId=%s", e, bidderId, templateId);
        }
        
        return null;
    }
    
    public void setLiveConfig(AAXServerLiveConfig liveConfig) {
        this.liveConfig = liveConfig;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Validate.notNull(liveConfig);
    }

    public void setAaxLiveConfigCreativeServiceErrorCounter(
            Counter aaxLiveConfigCreativeServiceErrorCounter) {
        this.aaxLiveConfigCreativeServiceErrorCounter = aaxLiveConfigCreativeServiceErrorCounter;
    }
}