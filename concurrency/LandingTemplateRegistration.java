/**
 * Copyright (c) 2014 Amazon.com, Inc.  All rights reserved.
 *
 * Owner: search-experience@
 */

package concurrency;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Teams that want to register their own templates should create an instance
 * of some class at runtime that:
 * - sets up the LandingTemplate instance as desired
 * - calls the static method on this class to register
 *
 * @author ludwick
 */
public class LandingTemplateRegistration
{
    private static final Log LOGGER = LogFactory.getLog(LandingTemplateRegistration.class);

    private static LandingTemplateRegistration INSTANCE = new LandingTemplateRegistration();

    /**
     * Get the singleton instance of this object.
     */
    public static LandingTemplateRegistration instance() 
    {
        return INSTANCE;
    }

    private Map<String, LandingTemplate> templates = new ConcurrentHashMap<>();

    /**
     * Register a new template by name and json specification. This method
     * intentionally swallows errors (with a log) on the assumption that users
     * of this feature will have tests confirming that their registration took
     * effect.
     *
     * @param name the name for the template (i.e. search-display-style)
     * @param jsonSpecification a string blob of json with fields from {@link LandingTemplate}
     */
    public void register(String name, String jsonSpecification)
    {
        if (templates.containsKey(name))
        {
            throw new IllegalStateException("Attempt to register a template twice for name '"+name+"'");
        }

        try
        {
            LandingTemplate template = LandingTemplate.fromJson(jsonSpecification);
            templates.put(name, template);
        }
        catch (final IOException | IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Attempt to register template with name " + name + " failed.", e);
        }
    }

    // All other methods package scope for use by LandingTemplateFactory
    //

    LandingTemplate find(String name)
    {
        if (StringUtils.isBlank(name))
        {
            return null;
        }

        return templates.get(name);
    }
}