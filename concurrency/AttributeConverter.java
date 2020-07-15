package concurrency;

import com.amazonaws.archipelago.dal.cdss.Constants;
import com.amazonaws.archipelago.dal.cdss.common.AttributeName;
import com.amazonaws.archipelago.dal.cdss.common.AttributeType;
import com.amazonaws.archipelago.dal.cdss.exception.UnsupportedEntityException;
import com.amazonaws.archipelago.dal.impl.clouddirectory.attributeconversion.*;
import com.amazonaws.archipelago.entity.ArchipelagoEntity;
import com.amazonaws.archipelago.entity.GroupEntity;
import com.amazonaws.archipelago.entity.UserEntity;
import com.amazonaws.archipelago.entity.UserPoolEntity;
import com.amazonaws.archipelago.entity.annotation.PersistedAttribute;
import com.amazonaws.archipelago.entity.attribute.*;
import com.amazonaws.archipelago.exception.NoAttributeConverterException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contain all methods for Tekton -- Archipelago attribute name/value conversion.
 */
public class AttributeConverter {
    private final Logger log_ = LogManager.getLogger(AttributeConverter.class);
    private final Map<Class<? extends ArchipelagoEntity>, List<NamedAttributeConverter>> classToNamedAttributeConverterMap_;
    private final Map<String, Pattern> regexPatternMap_;

    public AttributeConverter() {
        classToNamedAttributeConverterMap_ = new ConcurrentHashMap<>();
        regexPatternMap_ = new ConcurrentHashMap<>();

        initAttributeConverters(UserPoolEntity.class);
        initAttributeConverters(UserEntity.class);
        initAttributeConverters(GroupEntity.class);
    }

    /**
     * Transforms a Persisted Attribute name to the corresponding CD long attribute name including namespace.
     * We expect CD to migrate existing Storage Fabric attribute names unchanged to Tekton, including ":" character.
     * TODO, pending Tekton to lift restriction on ":" character in attribute name.
     * Until then, workaround for testing purposes is to substitute ":" with "-". E.g. "ident-user-id"
     *
     * @param entityCls the class for entity
     * @param shortAttributeName Attribute name in Archipelago entity
     * @return Tekton attribute name
     */
    public String persistedAttbNameToTektonAttbName(Class<? extends ArchipelagoEntity> entityCls, String shortAttributeName) {
        ArchipelagoEntity entity = ArchipelagoEntityClassMapping.getEntityForTypeBindedMethod(entityCls);
        String storageFabricAttributeName = entity.resolveArchipelagoAttributeName(shortAttributeName);
        String tektonAttributeName = storageFabricAttributeName.replaceFirst(":", "-").replaceFirst(":", "-");
        return tektonAttributeName;
    }

    /**
     * Transform shema attribute name into tekton format
     * @param entityCls the class for entity
     * @param shortAttributeName Schema attribute name in Archipelago entity
     * @return Tekton attribute name
     */
    public String schemaAttbNameToTektonAttbName(Class<? extends ArchipelagoEntity> entityCls, String shortAttributeName) {
        ArchipelagoEntity entity = ArchipelagoEntityClassMapping.getEntityForTypeBindedMethod(entityCls);
        String storageFabricAttributeName = entity.resolveSchemaAttributeName(shortAttributeName);
        String tektonAttributeName = storageFabricAttributeName.replaceFirst(":", "-").replaceFirst(":", "-").replaceFirst(":", "-");
        return tektonAttributeName;
    }

    /**
     * Convert case insensitive attributes' name into cdss internal format (shadow attribute).
     *
     * @param entityCls the class for entity
     * @param shortAttributeName Attribute name in Archipelago entity
     * @return Tekton shadow attribute name
     */
    public String attbNameToCdCaseInsensitiveShadowAttbName(Class<? extends ArchipelagoEntity> entityCls, String shortAttributeName) {
        ArchipelagoEntity entity = ArchipelagoEntityClassMapping.getEntityForTypeBindedMethod(entityCls);
        StringBuilder sb = new StringBuilder(ArchipelagoEntity.ARCHIPELAGO_ATTRIBUTE_NAMESPACE);
        sb.append(Constants.ATTRIBUTE_DELIMITER);
        sb.append(entity.getTypeName());
        sb.append(Constants.ATTRIBUTE_DELIMITER);
        sb.append(ArchipelagoEntity.SCHEMA_SHADOW_ATTRIBUTE_IDENTIFIER);
        sb.append(Constants.ATTRIBUTE_DELIMITER);
        sb.append(shortAttributeName);
        return sb.toString();
    }

    /**
     * Convert attributeValue to tekton's shadow value
     * @param attributeValue attributeValue need to be changed
     * @return tekton's shadow value
     */
    public String valueToCaseInsensitiveIndexedAttributeShadowValue(@Nullable String attributeValue) {
        if (StringUtils.isEmpty(attributeValue)) {
            return StringUtils.EMPTY;
        }

        return attributeValue.toLowerCase();
    }

    /**
     * Transforms a CD attribute name, to a corresponding AttributeType and short attribute name.
     * The type of Archipelago attribute is determined by the pattern of the CD attribute name.
     *
     * @param tektonAttributeName the attribute name stored in tekton
     *                            e.g. "ident-user-name", "ident-user-x-name", "ident-user-y-givenname"
     * @return AttributeName containing attributeType, tektonName and shortName
     */
    public AttributeName tektonAttbNameToAttributeName(String tektonAttributeName) {

        // TODO review regex after CD lifts the restriction on ":" character.
        final String customerCaseInsensitiveAttbRegex = "ident-[^-]*-y-(.*)";
        final String customerAttbRegex = "ident-[^-]*-x-(.*)";
        final String persistedAttbRegex = "ident-[^-]*-(.*)";

        Matcher customerCaseInsensitiveAttbMatcher = buildPatternMatcher(customerCaseInsensitiveAttbRegex, tektonAttributeName);
        Matcher customerAttbMatcher = buildPatternMatcher(customerAttbRegex, tektonAttributeName);
        Matcher persistedAttbMatcher = buildPatternMatcher(persistedAttbRegex, tektonAttributeName);

        AttributeType attributeType;
        String shortAttributeName;

        if (customerCaseInsensitiveAttbMatcher.find()) {
            attributeType = AttributeType.ARCHIPELAGO_SCHEMA_CASE_INSENSITIVE;
            shortAttributeName = customerCaseInsensitiveAttbMatcher.group(1);
        } else if (customerAttbMatcher.find()) {
            attributeType = AttributeType.ARCHIPELAGO_SCHEMA;
            shortAttributeName = customerAttbMatcher.group(1);
        } else if (persistedAttbMatcher.find()) {
            attributeType = AttributeType.PERSISTED;
            shortAttributeName = persistedAttbMatcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid Tekton attribute name: " + tektonAttributeName);
        }
        return new AttributeName(attributeType, shortAttributeName, tektonAttributeName);
    }

    /**
     * Using an ArchipelagoEntity and a list of schema defined attributes, this method produces a list of Strings that
     * represent each attribute's name in Tekton. Attributes from the entity are found by the PersistedAttribute annotation.
     *
     * @param entityCls the class for entity
     * @param requestedAttributes List of attribute names to include. These should be the raw name, not the transformed
     *                            version.
     * @return A List of Strings containing the Tekton representation of the necessary and requested attributes.
     */
    public List<String> getTektonAttributeNames(final Class<? extends ArchipelagoEntity> entityCls,
            final List<String> requestedAttributes) {
        // Transform all passed attributes keys into the CD form
        final List<String> attributeKeys = new ArrayList<>();
        for (final String requestedAttribute : requestedAttributes) {
            attributeKeys.add(schemaAttbNameToTektonAttbName(entityCls, requestedAttribute));
        }

        for (final NamedAttributeConverter converter : classToNamedAttributeConverterMap_.get(entityCls)) {
            final Field field = converter.getAttributeField();
            final PersistedAttribute annotation = field.getAnnotation(PersistedAttribute.class);

            if (annotation != null && annotation.isAttributeSplit() == 1) {
                final Optional<AbstractLargeAttributeSplitAndCombinerStrategy> splitAndCombinerOptional =
                        converter.getSplitAndCombiner();
                if (splitAndCombinerOptional.isPresent()) {
                    final List<String> internalNames = splitAndCombinerOptional.get().listSplitAttributeNames();

                    internalNames.forEach(element -> attributeKeys.add(
                            persistedAttbNameToTektonAttbName(entityCls, element)));
                }
            } else {
                attributeKeys.add(
                        persistedAttbNameToTektonAttbName(entityCls, converter.getAttributeName()));
            }
        }

        return attributeKeys;
    }

    /**
     * package scoped method, used by AttributeXxx utility only
     * Get a list of NamedAttributeConverter for each persisted attribute in entity
     * @param entityCls entity class
     * @return The unmodifiable list of NamedAttributeConverter for each persisted attribute in entity
     * @throws UnsupportedEntityException The entity is not supported (only UserPoolEntity, UserEntity and GroupEntity are supported)
     */
    List<NamedAttributeConverter> getPersistedAttributeConverters(Class<? extends ArchipelagoEntity> entityCls)
        throws UnsupportedEntityException {
        if (!classToNamedAttributeConverterMap_.containsKey(entityCls)) {
            throw new UnsupportedEntityException(
                    String.format(
                            "Persisted attribute converters for entity %s are not initialized",
                            entityCls.getSimpleName())
            );
        }

        return Collections.unmodifiableList(classToNamedAttributeConverterMap_.get(entityCls));
    }

    public AbstractAttributeConverterStrategy getConverterStrategyForAttributeDataType(Class attributeDataTypeCls) {
        return AVAILABLE_CONVERTERS.get(attributeDataTypeCls);
    }

    private void initAttributeConverters(Class<? extends ArchipelagoEntity> entityCls)
            throws NoAttributeConverterException {
        if (classToNamedAttributeConverterMap_.containsKey(entityCls)) {
            log_.info("entity {}'s persisted attribute converters are already initialized", entityCls.getSimpleName());
            return;
        }

        List<NamedAttributeConverter> namedAttributeConverters = new ArrayList<>();
        log_.info("Initialize persisted attribute converters for entity {}", entityCls.getSimpleName());
        Class c = entityCls;
        while (c != null) {
            for (Field fu : c.getDeclaredFields()) {
                PersistedAttribute annotation = fu.getAnnotation(PersistedAttribute.class);
                if (annotation == null) {
                    continue;
                }
                fu.setAccessible(true);

                if (annotation.isAttributeSplit() == 1) {
                    if (!AVAILABLE_SPLIT_AND_CONBINERS.containsKey(fu.getType())) {
                        throw new NoAttributeConverterException(
                                "No splitAndCombiner converter found for attribute type and annotation is set to isAttributeSplit: " + fu.getType().getCanonicalName());
                    }
                    AbstractLargeAttributeSplitAndCombinerStrategy splitAndCombiner = AVAILABLE_SPLIT_AND_CONBINERS.get(fu.getType());
                    namedAttributeConverters.add(new NamedAttributeConverter(fu, annotation.name(), splitAndCombiner));
                 } else {
                    if (!AVAILABLE_CONVERTERS.containsKey(fu.getType())) {
                        throw new NoAttributeConverterException(
                                "No converter found for attribute type: " + fu.getType().getCanonicalName());
                    }
                    AbstractAttributeConverterStrategy converter = AVAILABLE_CONVERTERS.get(fu.getType());
                    namedAttributeConverters.add(new NamedAttributeConverter(fu, annotation.name(), converter));
                }
            }
            // Go up the object hierarchy
            c = c.getSuperclass();
        }

        classToNamedAttributeConverterMap_.put(entityCls, namedAttributeConverters);
    }

    private Matcher buildPatternMatcher(String regex, String s) {
        Pattern p = regexPatternMap_.computeIfAbsent(regex, r -> Pattern.compile(r));
        return p.matcher(s);
    }

    private static final Map<Class, AbstractAttributeConverterStrategy> AVAILABLE_CONVERTERS =
            new HashMap<Class, AbstractAttributeConverterStrategy>() {{
                put(BigInteger.class, new BigIntegerAttributeConverterStrategy());
                put(Date.class, new DateAttributeConverterStrategy());
                put(Integer.class, new IntegerAttributeConverterStrategy());
                put(Boolean.class, new BooleanAttributeConverterStrategy());
                put(PasswordPolicy.class, new PasswordPolicyAttributeConverterStrategy());
                put(Signature.class, new SignatureAttributeConverterStrategy());
                put(Status.class, new StatusAttributeConverterStrategy());
                put(String.class, new StringAttributeConverterStrategy());
                put(EmailConfiguration.class, new EmailConfigurationAttributeConverterStrategy());
                put(SmsConfiguration.class, new SmsConfigurationAttributeConverterStrategy());
                put(GroupMembershipIdSet.class, new GroupMembershipIdSetAttributeConverterStrategy());
            }};

    private static final Map<Class, AbstractLargeAttributeSplitAndCombinerStrategy> AVAILABLE_SPLIT_AND_CONBINERS =
            new HashMap<Class, AbstractLargeAttributeSplitAndCombinerStrategy>() {{
                put(PasswordSaltAndVerifierHistory.class, new PasswordSaltAndVerifierHistorySplitAndCombinerStrategy());
            }};

}