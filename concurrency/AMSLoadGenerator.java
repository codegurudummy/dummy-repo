package concurrency;

import amazon.wapqa.tpsgenerator.client.annotations.TPSGenerator;
import amazon.wapqa.tpsgenerator.client.annotations.TPSGeneratorDataProvider;
import amazon.wapqa.tpsgenerator.client.annotations.TPSGeneratorInitialize;
import amazon.wapqa.tpsgenerator.client.annotations.TPSGeneratorTransaction;
import amazon.wapqa.tpsgenerator.client.domain.FileType;
import com.amazon.amsqa.client.AMSClientFactory;
import com.amazon.amsqa.helpers.AMSRequestProvider;
import com.amazon.amsqa.helpers.Constants;
import com.amazon.amsqa.helpers.RetrievePermissionDataIterator;
import com.amazon.coral.validate.ValidationException;
import com.amazon.dee.sdk.application.management.*;
import com.amazon.dee.sdk.application.management.ManifestVariant;
import com.amazon.dee.sdk.application.management.impl.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LoadGenator class for DeeApplicationManagementService
 *
 * @author meesalam
 */
@TPSGenerator("AMSLoadGen")
public class AMSLoadGenerator {
    // Fields
    private DeeApplicationManagementServiceClient amsClient;
    private AMSRequestProvider requestProvider;

    private static final int APPID_AND_STAGE_APPID_INDEX = 0;
    private static final int APPID_AND_STAGE_STAGE_INDEX = 1;

    private static final int ENABLEMENT_SKILLID_INDEX = 0;
    private static final int ENABLEMENT_STAGE_INDEX = 1;
    private static final int ENABLEMENT_CUSTID_INDEX = 2;

    private static final int RETRIEVE_ENTITLED_SKILLS_CUSTOMER_ID_INDEX = 0;
    private static final int RETRIEVE_ENTITLED_SKILLS_SKILL_TYPES_INDEX = 1;

    private static final int RETRIEVE_ENABLED_SKILLS_SKILL_ID_FILTER_INDEX = 2;
    private static final int RETRIEVE_ENABLED_SKILLS_MAX_PAGE_SIZE_INDEX = 3;
    private static final int RETRIEVE_ENABLED_SKILLS_NEXT_PAGE_START_KEY_INDEX = 4;
    private static final int RETRIEVE_ENABLED_SKILLS_PAGINATION_REQUESTED_INDEX = 5;

    private static final int ENABLE_SKILL_STAGES_CUSTID_INDEX = 0;
    private static final int ENABLE_SKILL_STAGES_SKILLIDS_INDEX = 1;
    private static final int ENABLE_SKILL_STAGES_STAGES_INDEX = 2;

    private static final int RETRIEVE_ENABLEMENT_BY_ASIN_CUSTID_INDEX = 0;
    private static final int RETRIEVE_ENABLEMENT_BY_ASIN_ASIN_INDEX = 1;

    private static final int ENABLEMENT_BY_ASIN_CUSTID_INDEX = 0;
    private static final int ENABLEMENT_BY_ASIN_ASIN_INDEX = 1;

    private static final int DISABLEMENT_BY_ASIN_CUSTID_INDEX = 0;
    private static final int DISABLEMENT_BY_ASIN_ASIN_INDEX = 1;

    private static final List<String> FEATURE_KEY_LIST = Arrays.asList("*");

    private RetrievePermissionDataIterator retrievePermissionDataIterator;

    @TPSGeneratorInitialize
    public void initialize(final String[] additionalCliArgs) throws Exception {
        // Initializing the domain and setting the user directory
        CommandLine commandLine = parseParametersFromCLI(additionalCliArgs);

        String aaaEnvironment = commandLine.getOptionValue(Constants.AAA_ENVIRONMENT_PARAMETER);
        if (aaaEnvironment == null) {
            aaaEnvironment = Constants.DEFAULT_AAA_ENVIRONMENT;
        }
        System.out.println("AAA environment: " + aaaEnvironment);
        System.setProperty("user.dir", aaaEnvironment);

        String domain = commandLine.getOptionValue(Constants.DOMAIN_PARAMETER);
        if (! Constants.SUPPORTED_DOMAINS.contains(domain)) {
            throw new UnrecognizedOptionException("\"" + domain
            + "\" is not a valid value for parameter \""
            + Constants.DOMAIN_PARAMETER + "\"");
        }
        System.out.println("Domain: " + domain);
        amsClient = AMSClientFactory.createClient(domain);

        requestProvider = new AMSRequestProvider();
        retrievePermissionDataIterator = new RetrievePermissionDataIterator(Constants.S3_KEY);
    }

    /***
     * Method to retrieve parsed parameters from the extra CLI arguments
     *
     * @param additionalCliArgs the arguments to parse
     * @return the parsed command line
     * @throws ParseException if an error occurs while parsing
     */
    @SuppressWarnings("static-access")
    public CommandLine parseParametersFromCLI(String[] additionalCliArgs)
    throws ParseException {
        final Options options = new Options();
        final Parser parser = new GnuParser();

        options.addOption(OptionBuilder.hasArg()
        .withDescription("Environment for AAA state, for instance"
        + " /apollo/env/DeeApplicationManagementService")
        .create(Constants.AAA_ENVIRONMENT_PARAMETER));

        options.addOption(OptionBuilder.hasArg().isRequired()
        .withDescription("Stage you want to hit, one of Beta, Gamma or Prod")
        .create(Constants.DOMAIN_PARAMETER));

        return parser.parse(options, additionalCliArgs);
    }

    /**
     * Invokes {@code EnableApplication} and {@code DisableApplication}.
     *
     * @param customerIdApplicationId a customer ID and a skill ID for enabling / disabling
     */
    @TPSGeneratorTransaction(value = "EnableDisableApplication",
                             dataProvider = "EnableDisableApplicationCustomerIdsApplicationIds")
    public void enableAndDisableApplication(final String[] customerIdApplicationId) {
        enableApplication(customerIdApplicationId[ENABLEMENT_CUSTID_INDEX],
                          customerIdApplicationId[ENABLEMENT_SKILLID_INDEX]);
        disableApplication(customerIdApplicationId[ENABLEMENT_CUSTID_INDEX],
                           customerIdApplicationId[ENABLEMENT_SKILLID_INDEX]);
    }

    /**
     * Invokes {@code EnableApplication}.
     */
    @TPSGeneratorTransaction(value = "EnableApplicationBySkillId")
    public void enableApplicationBySkillId() {
        String[] ids = retrievePermissionDataIterator.next();
        System.out.println("!!!!! EnableApplicationBySkillId: " + ids[RetrievePermissionDataIterator.CUSTOMER_ID_INDEX] + "; " + ids[RetrievePermissionDataIterator.SKILL_ID_INDEX]);
        enableApplication(
            ids[RetrievePermissionDataIterator.CUSTOMER_ID_INDEX],
            ids[RetrievePermissionDataIterator.SKILL_ID_INDEX]
        );
    }

    /**
     * Invokes {@code RetrievePermissionsValidationContext}.
     */
    @TPSGeneratorTransaction(value = "RetrievePermissionsValidationContext",
                             dataProvider = "RetrievePermissionsValidationContextByParams")
    public void retrievePermissionsValidationContext(String[] input) {
        String[] csvRow = Arrays.copyOf(input, 3);
        RetrievePermissionsValidationContextRequest request =
                new RetrievePermissionsValidationContextRequest();
        if (StringUtils.isNotEmpty(csvRow[0])) {
            request.setUserId(csvRow[0]);
        }
        if (StringUtils.isNotEmpty(csvRow[1])) {
            request.setClientId(csvRow[1]);
        }
        if (StringUtils.isNotEmpty(csvRow[2])) {
            request.setCustomerId(csvRow[2]);
        }

        final RetrievePermissionsValidationContextCall caller =
            amsClient.newRetrievePermissionsValidationContextCall();
        try {
            caller.call(request);
        } catch (SkillStageDoesNotExistException | EnablementDoesNotExistException | ValidationException ex) {
            System.out.println("RetrievePermissionsValidationContext failed: " + ex);
        }
    }

    /**
     * Invokes {@code EnableApplicationByAsin}
     *
     * @param customerIdAsin a customer ID and an ASIN for enabling
     */
    @TPSGeneratorTransaction(value = "EnableApplicationByAsin",
                             dataProvider = "EnableByAsinCustomerIdsAsins")
    public void enableApplicationByAsin(final String[] customerIdAsin) {

        final EnableApplicationByAsinRequest enableApplicationByAsinRequest =
            requestProvider.getEnableApplicationByAsinRequest();
        final EnableApplicationByAsinCall enableApplicationByAsinCall =
            amsClient.newEnableApplicationByAsinCall();

        enableApplicationByAsinRequest.setCustomerId(customerIdAsin[ENABLEMENT_BY_ASIN_CUSTID_INDEX].trim());
        enableApplicationByAsinRequest.setAsin(customerIdAsin[ENABLEMENT_BY_ASIN_ASIN_INDEX].trim());

        enableApplicationByAsinCall.call(enableApplicationByAsinRequest);

    }

    /**
     * Invokes {@code DisableApplicationByAsin}
     *
     * @param customerIdAsin a customer ID and an ASIN for disabling
     */
    @TPSGeneratorTransaction(value = "DisableApplicationByAsin",
                             dataProvider = "DisableByAsinCustomerIdsAsins")
    public void disableApplicationByAsin(final String[] customerIdAsin) {

        final DisableApplicationByAsinRequest disableApplicationByAsinRequest =
            requestProvider.getDisableApplicationByAsinRequest();
        final DisableApplicationByAsinCall disableApplicationByAsinCall =
            amsClient.newDisableApplicationByAsinCall();

        disableApplicationByAsinRequest.setCustomerId(customerIdAsin[DISABLEMENT_BY_ASIN_CUSTID_INDEX].trim());
        disableApplicationByAsinRequest.setAsin(customerIdAsin[DISABLEMENT_BY_ASIN_ASIN_INDEX].trim());

        disableApplicationByAsinCall.call(disableApplicationByAsinRequest);
    }

    /**
     * Invokes {@code RetrieveEnablement}.
     *
     * @param customerIdApplicationIdStage a customer ID, a skill ID and a stage to retrieve
     */
    @TPSGeneratorTransaction(value = "RetrieveEnablement",
                             dataProvider = "RetrieveEnablementSkillIdsStagesCustomerIds")
    public void retrieveEnablement(final String[] customerIdApplicationIdStage) {
        final  RetrieveEnablementRequest retrieveEnablementRequest =
            requestProvider.getRetrieveEnablementRequest();
        final RetrieveEnablementCall retrieveEnablementCall =
            amsClient.newRetrieveEnablementCall();

        retrieveEnablementRequest.setApplicationId(
            customerIdApplicationIdStage[ENABLEMENT_SKILLID_INDEX]);
        if (StringUtils.isNotEmpty(customerIdApplicationIdStage[ENABLEMENT_STAGE_INDEX])) {
            retrieveEnablementRequest.setStage(customerIdApplicationIdStage[ENABLEMENT_STAGE_INDEX]);
        }
        retrieveEnablementRequest.setCustomerId(
            customerIdApplicationIdStage[ENABLEMENT_CUSTID_INDEX]);

        try {
            retrieveEnablementCall.call(retrieveEnablementRequest);
        } catch (EnablementDoesNotExistException ex) {
            System.out.println("RetrieveEnablement failed: " + ex);
        }
    }

    /**
     * Invokes {@code RetrieveEnabledSkillsInformation}.
     *
     * @param input - CustomerId, SkillTypes, FeatureKeys, SkillIdFilter, MaxPageSize, NextPageKey
     */
    @TPSGeneratorTransaction(value = "RetrieveEnabledSkillsInformation",
            dataProvider = "RetrieveEnabledSkillsInformationByParams")
    public void retrieveEnabledSkillsInformation(final String[] input) {
        RetrieveEnabledSkillsInformationRequest retrieveEnabledSkillsInformationRequest =
                new RetrieveEnabledSkillsInformationRequest();
        String[] csvRow = Arrays.copyOf(input, 6);
        RetrieveEnabledSkillsInformationCall retrieveEnabledSkillsInformationCall =
                amsClient.newRetrieveEnabledSkillsInformationCall();
        String customerId = csvRow[0];
        List<String> skillTypes = getStringListFromRequest(csvRow[1]);
        List<String> featureKeys = getStringListFromRequest(csvRow[2]);
        List<String> skillIdFilter = getStringListFromRequest(csvRow[3]);

        retrieveEnabledSkillsInformationRequest.setCustomerId(customerId);
        retrieveEnabledSkillsInformationRequest.setSkillTypes(skillTypes);
        retrieveEnabledSkillsInformationRequest.setFeatureKeys(featureKeys);
        retrieveEnabledSkillsInformationRequest.setSkillIdFilter(skillIdFilter);
        if (StringUtils.isNotEmpty(csvRow[4])) {
            retrieveEnabledSkillsInformationRequest.setMaxPageSize(Integer.parseInt(csvRow[4]));
        }
        if (StringUtils.isNotEmpty(csvRow[5])) {
            retrieveEnabledSkillsInformationRequest.setNextPageKey(csvRow[5]);
        }
        try {
            retrieveEnabledSkillsInformationCall.call(retrieveEnabledSkillsInformationRequest);
        } catch (ValidationException ex) {
            System.out.println("RetrieveEnabledSkillsInformation failed: " + ex);
        }
    }

    /**
     * Invokes {@code RetrieveEntitledSkill} to retrieve skills.
     *
     * @param input the skillIds and skill types to retrieve.
     */
    @TPSGeneratorTransaction(value = "RetrieveEntitledSkills",
                             dataProvider = "RetrieveEntitledSkillsByParams")
    public void retrieveEntitledSkills(final String[] input) {
        String[] csvRow = Arrays.copyOf(input, 2);
        RetrieveEntitledSkillsRequest retrieveEntitledSkillsRequest =
            requestProvider.getRetrieveEntitledSkillsRequest();

        RetrieveEntitledSkillsCall retrieveEntitledSkillsCall =
            amsClient.newRetrieveEntitledSkillsCall();
        String customerId = csvRow[RETRIEVE_ENTITLED_SKILLS_CUSTOMER_ID_INDEX];
        List<String> skillTypes = getStringListFromRequest(csvRow[RETRIEVE_ENTITLED_SKILLS_SKILL_TYPES_INDEX]);
        retrieveEntitledSkillsRequest.setCustomerId(customerId);
        retrieveEntitledSkillsRequest.setSkillTypes(skillTypes);
        retrieveEntitledSkillsRequest.setPublishedSkillNeeded(true);
        retrieveEntitledSkillsCall.call(retrieveEntitledSkillsRequest);
    }

    /**
     * Invokes {@code RetrieveEntitledSkill} to retrieve skills.
     *
     * @param input - CustomerId, SkillTypes, SkillIdFilter, MaxPageSize, NextPageStartKey,
     *                PaginationRequested
     */
    @TPSGeneratorTransaction(value = "RetrieveEnabledSkills",
                             dataProvider = "RetrieveEnabledSkillsByParams")
    public void retrieveEnabledSkills(final String[] input) {
        String[] csvRow = Arrays.copyOf(input, 6);
        RetrieveEnabledSkillsRequest retrieveEnabledSkillsRequest =
            requestProvider.getRetrieveEnabledSkillsRequest();

        RetrieveEnabledSkillsCall retrieveEntitledSkillsCall =
            amsClient.newRetrieveEnabledSkillsCall();
        String customerId = csvRow[RETRIEVE_ENTITLED_SKILLS_CUSTOMER_ID_INDEX];
        List<String> skillTypes = getStringListFromRequest(
                csvRow[RETRIEVE_ENTITLED_SKILLS_SKILL_TYPES_INDEX]);
        List<String> skillIdFilter = getStringListFromRequest(
                csvRow[RETRIEVE_ENABLED_SKILLS_SKILL_ID_FILTER_INDEX]);

        retrieveEnabledSkillsRequest.setCustomerId(customerId);
        retrieveEnabledSkillsRequest.setSkillTypes(skillTypes);
        retrieveEnabledSkillsRequest.setSkillIdFilter(skillIdFilter);
        if (StringUtils.isNotEmpty(csvRow[RETRIEVE_ENABLED_SKILLS_MAX_PAGE_SIZE_INDEX])) {
            retrieveEnabledSkillsRequest.setMaxPageSize(
                    Integer.parseInt(csvRow[RETRIEVE_ENABLED_SKILLS_MAX_PAGE_SIZE_INDEX]));
        }
        if (StringUtils.isNotEmpty(csvRow[RETRIEVE_ENABLED_SKILLS_NEXT_PAGE_START_KEY_INDEX])) {
            retrieveEnabledSkillsRequest.setNextPageStartKey(
                    csvRow[RETRIEVE_ENABLED_SKILLS_NEXT_PAGE_START_KEY_INDEX]);
        }
        if (StringUtils.isNotEmpty(csvRow[RETRIEVE_ENABLED_SKILLS_PAGINATION_REQUESTED_INDEX])) {
            retrieveEnabledSkillsRequest.setPaginationNeeded(
                    new Boolean(csvRow[RETRIEVE_ENABLED_SKILLS_PAGINATION_REQUESTED_INDEX]));
        }
        retrieveEntitledSkillsCall.call(retrieveEnabledSkillsRequest);
    }

    /**
     * Invokes {@code RetrieveEnablementByAsin} to retrieve skills.
     *
     * @param csvRow the skillIds and skill types to retrieve.
     */
    @TPSGeneratorTransaction(value = "RetrieveEnablementByAsin",
                             dataProvider = "RetrieveEnablementByAsinCustomerIdsAsins")
    public void retrieveEnablementByAsin(final String[] csvRow) {
        RetrieveEnablementByAsinRequest retrieveEnabledSkillsRequest =
            requestProvider.getRetrieveEnablementByAsinRequest();

        RetrieveEnablementByAsinCall retrieveEntitledSkillsCall =
            amsClient.newRetrieveEnablementByAsinCall();
        String asin = csvRow[RETRIEVE_ENABLEMENT_BY_ASIN_ASIN_INDEX];
        String customerId = csvRow[RETRIEVE_ENABLEMENT_BY_ASIN_CUSTID_INDEX];
        retrieveEnabledSkillsRequest.setAsin(asin);
        retrieveEnabledSkillsRequest.setCustomerId(customerId);
        retrieveEntitledSkillsCall.call(retrieveEnabledSkillsRequest);
    }

    /**
     * Invokes {@code EnableSkillStages} to enable skills.
     * @param csvRow contains customerId, skillId, stage.
     */
    @TPSGeneratorTransaction(value = "EnableDisableSkillStages",
                             dataProvider = "EnableDisableSkillStagesCustomerIdsSkillIdsStages")
    public void enableAndDisableSkillStages(final String[] csvRow) {
        EnableSkillStagesRequest enableSkillStagesRequest = requestProvider.getEnableSkillStagesRequest();
        EnableSkillStagesCall enableSkillStagesCall =
            amsClient.newEnableSkillStagesCall();
        String customerId = csvRow[ENABLE_SKILL_STAGES_CUSTID_INDEX];
        List<String> skillIds = getStringListFromQuotes(csvRow[ENABLE_SKILL_STAGES_SKILLIDS_INDEX]);
        List<String> stages = getStringListFromQuotes(csvRow[ENABLE_SKILL_STAGES_STAGES_INDEX]);
        List<SkillStageEnablement> skillStageEnablements =
            buildSkillStageEnablementList(skillIds, stages);
        if (skillStageEnablements != null) {
            enableSkillStagesRequest.setCustomerId(customerId);
            enableSkillStagesRequest.setSkillStageEnablementList(skillStageEnablements);
            enableSkillStagesCall.call(enableSkillStagesRequest);
        }

        for (String skillId: skillIds) {
            disableApplication(customerId, skillId);
        }
    }

    /**
     * Invokes {@code RetrieveSkillStage} to get skill stage data.
     *
     * @param csvRow contains skillId and stage.
     */
    @TPSGeneratorTransaction(value = "RetrieveSkillStage",
                             dataProvider = "RetrieveSkillStageSkillIdsStages")
    public void retrieveSkillStage(final String[] csvRow) {
        RetrieveSkillStageRequest retrieveSkillStageRequest = requestProvider.getRetrieveSkillStageRequest();
        retrieveSkillStageRequest.setSkillId(csvRow[APPID_AND_STAGE_APPID_INDEX]);
        retrieveSkillStageRequest.setStage(csvRow[APPID_AND_STAGE_STAGE_INDEX]);
        RetrieveSkillStageCall retrieveSkillStageCall = amsClient.newRetrieveSkillStageCall();
        retrieveSkillStageCall.call(retrieveSkillStageRequest);
    }

    /**
     * Invokes {@code SaveSkillStage} to save skillStageInformation.
     *
     * @param input List of vendorId, stage, skillTypes(separated by '$'), external manifest and internal manifest
     */
    @TPSGeneratorTransaction(value = "SaveSkillStage",
                             dataProvider = "SaveSkillStageFileReader")
    public void saveSkillStage(final List<String> inputList) {
        List<String> skillTypes = Arrays.asList(inputList.get(2).split("$"));
        SaveSkillStageRequest saveSkillStageRequest = new SaveSkillStageRequest()
        .builder()
        .withVendorId(inputList.get(0))
        .withStage(inputList.get(1))
        .withSkillTypes(skillTypes)
        .withExternalManifestFeatures(inputList.get(3))
        .withInternalManifestFeatures(inputList.get(4))
        .withSkillDefinition("")
        .build();
        SaveSkillStageCall saveSkillStageCall = amsClient.newSaveSkillStageCall();
        saveSkillStageCall.call(saveSkillStageRequest);
    }

    /**
     * Invokes {@code RetrieveSkills} to get skill Information.
     *
     * @param input - SkillStageIdentifiers, FeatureKeys
     */
    @TPSGeneratorTransaction(value = "RetrieveSkills",
                             dataProvider = "RetrieveSkillsByParams")
    public void retrieveSkills(final String[] input) {
        String[] csvRow = Arrays.copyOf(input, 2);
        RetrieveSkillsRequest retrieveSkillsRequest = new RetrieveSkillsRequest()
        .builder()
        .withFeatureKeys(getStringListFromRequest(csvRow[1]))
        .withManifestVariant(ManifestVariant.MERGED_MANIFESTS)
        .withSkillStageIdentifiers(getSkillStageIdentifierList(csvRow[0]))
        .withCacheUsageAllowed(true)
        .build();
        RetrieveSkillsCall retrieveSkillsCall = amsClient.newRetrieveSkillsCall();
        retrieveSkillsCall.call(retrieveSkillsRequest);
    }

    /**
     * Invokes {@code RetrieveSkillsByAsins} to get skill Information.
     *
     * @param asinsList List of asins
     */
    @TPSGeneratorTransaction(value = "RetrieveSkillsByAsins",
                             dataProvider = "RetrieveSkillsByAsinsAsinList")
    public void retrieveSkillsByAsins(final List<String> asinsList) {
        RetrieveSkillsByAsinsRequest retrieveSkillsByAsinsRequest = new RetrieveSkillsByAsinsRequest()
        .builder()
        .withFeatureKeys(FEATURE_KEY_LIST)
        .withManifestVariant(ManifestVariant.MERGED_MANIFESTS)
        .withAsins(asinsList)
        .build();
        RetrieveSkillsByAsinsCall retrieveSkillsByAsinsCall = amsClient.newRetrieveSkillsByAsinsCall();
        retrieveSkillsByAsinsCall.call(retrieveSkillsByAsinsRequest);
    }
    
    /**
     * Invokes {@code RetrieveSkillsByStatus} to get skill Information.
     *
     * @param vendorId vendorId
     */
    @TPSGeneratorTransaction(value = "RetrieveSkillsByStatus",
                             dataProvider = "RetrieveSkillsByStatusStatusList")
    public void retrieveSkillsByStatus(final String status) {
        RetrieveSkillsByStatusRequest retrieveSkillsByStatusRequest = requestProvider.getRetrieveSkillsByStatusRequest();
        retrieveSkillsByStatusRequest.setFeatureKeys(FEATURE_KEY_LIST);
        retrieveSkillsByStatusRequest.setManifestVariant(ManifestVariant.MERGED_MANIFESTS);
        retrieveSkillsByStatusRequest.setStatus(status);
        RetrieveSkillsByStatusCall retrieveSkillsByStatusCall = amsClient.newRetrieveSkillsByStatusCall();
        retrieveSkillsByStatusCall.call(retrieveSkillsByStatusRequest);
    }

    /**
     * Invokes {@code RetrieveSkillsByVendor} to get skill Information.
     *
     * @param vendorId vendorId
     */
    @TPSGeneratorTransaction(value = "RetrieveSkillsByVendor",
                             dataProvider = "RetrieveSkillsByVendorVendorIds")
    public void retrieveSkillsByVendor(final String vendorId) {
        RetrieveSkillsByVendorRequest retrieveSkillsByVendorRequest = new RetrieveSkillsByVendorRequest()
        .builder()
        .withFeatureKeys(FEATURE_KEY_LIST)
        .withManifestVariant(ManifestVariant.MERGED_MANIFESTS)
        .withVendorId(vendorId)
        .build();
        RetrieveSkillsByVendorCall retrieveSkillsByVendorCall = amsClient.newRetrieveSkillsByVendorCall();
        retrieveSkillsByVendorCall.call(retrieveSkillsByVendorRequest);
    }

    /**
     * Invokes {@code EnableApplication}.
     *
     * @param customerId the customer ID to enable the skill for
     * @param applicationId the skill ID to enable
     */
    private void enableApplication(final String customerId, final String applicationId) {
        final EnableApplicationRequest enableApplicationRequest =
            requestProvider.getEnableApplicationRequest();
        final EnableApplicationCall enableApplicationCall =
            amsClient.newEnableApplicationCall();

        enableApplicationRequest.setCustomerId(customerId);
        enableApplicationRequest.setApplicationId(applicationId);

        enableApplicationCall.call(enableApplicationRequest);
    }

    /**
     * Invokes {@code DisableApplication}.
     *
     * @param customerId the customer ID to disable the skill for
     * @param applicationId the skill ID to disable
     */
    private void disableApplication(final String customerId, final String applicationId) {
        final DisableApplicationRequest disableApplicationRequest =
            requestProvider.getDisableApplicationRequest();
        final DisableApplicationCall disableApplicationCall =
            amsClient.newDisableApplicationCall();

        disableApplicationRequest.setCustomerId(customerId);
        disableApplicationRequest.setApplicationId(applicationId);

        disableApplicationCall.call(disableApplicationRequest);
    }

    /***
     * Data provider method for {@code EnableApplication} and {@code DisableApplication}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID and skill ID
     */
    @TPSGeneratorDataProvider(name = "EnableDisableApplicationCustomerIdsApplicationIds",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.ENABLE_DISABLE_APPLICATION_CUSTOMER_IDS_APPLICATION_IDS)
    public String[] getEnableDisableApplicationCustomerIdsApplicationIds(final String[] csvRow) {
        return csvRow;
    }

    /**
     *  Data provider for {@code RetrievePermissionsValidationContext}
     * @param csvRow - UserId, ClientId, CustomerId
     * @return
     */
    @TPSGeneratorDataProvider(name = "RetrievePermissionsValidationContextByParams",
                              inputFileType = FileType.TAB_DELIMITED,
                              fileLocation = Constants.RETRIEVE_PERMISSIONS_VALIDATION_CONTEXT_FILE)
    public String[] getRetrievePermissionsValidationContextByParams(final String[] csvRow) {
        return csvRow;
    }

    /***
     * Data provider method for {@code EnableApplicationByAsin}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID and Asin
     */
    @TPSGeneratorDataProvider(name = "EnableByAsinCustomerIdsAsins",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.ENABLE_APPLICATION_BY_ASIN_CUSTOMER_IDS_ASINS)
    public String[] getEnableApplicationByAsinCustomerIdsAsins(final String[] csvRow) {
        return csvRow;
    }

    /***
     * Data provider method for {@code DisableApplicationByAsin}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID and Asin
     */
    @TPSGeneratorDataProvider(name = "DisableByAsinCustomerIdsAsins",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.DISABLE_APPLICATION_BY_ASIN_CUSTOMER_IDS_ASINS)
    public String[] getDisableApplicationByAsinCustomerIdsAsins(final String[] csvRow) {
        return csvRow;
    }
    /**
     * Data provider method for {@code RetrieveSkillStage}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a skill ID and a stage
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillStageSkillIdsStages",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.RETRIEVE_SKILL_STAGE_SKILL_IDS_STAGES_FILE)
    public String[] getRetrieveSkillStageSkillIdsStages(final String[] csvRow) {
        return csvRow;
    }

    /**
     * Data provider method for {@code RetrieveEntitleSkills}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID, skill types
     */
    @TPSGeneratorDataProvider(name = "RetrieveEntitledSkillsByParams",
                              inputFileType = FileType.TAB_DELIMITED,
                              fileLocation = Constants.RETRIEVE_ENTITLED_SKILLS_FILE)
    public String[] getRetrieveEntitledSkillsByParams(final String[] csvRow) {
        return csvRow;
    }

    /**
     * Data provider method for {@code EnableSkillStages}.
     *
     * @param csvRow items from a row in the CSV file.
     * @return array containing a customer ID, skill IDs(separated by '$'), skill stages(separated by '$').
     */
    @TPSGeneratorDataProvider(name = "EnableDisableSkillStagesCustomerIdsSkillIdsStages",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.ENABLE_SKILL_STAGES_CUSTOMER_IDS_SKILL_IDS_STAGES_FILE)
    public String[] getEnableDisableSkillStagesCustomerIdsSkillIdsStages(final String[] csvRow) {
        return csvRow;
    }

    /**
     * Data provider method for {@code RetrieveEnabledSkills}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID, skill types
     */
    @TPSGeneratorDataProvider(name = "RetrieveEnabledSkillsByParams",
                              inputFileType = FileType.TAB_DELIMITED,
                              fileLocation = Constants.RETRIEVE_ENABLED_SKILLS_FILE)
    public String[] getRetrieveEnabledSkillsByParams(final String[] csvRow) {
        return csvRow;
    }


    /**
     * Data provider method for {@code RetrieveEnabledSkills}.
     *
     * @param csvRow items from a row in the CSV file
     * @return array containing a customer ID, skill types
     */
    @TPSGeneratorDataProvider(name = "RetrieveEnablementSkillIdsStagesCustomerIds",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.RETRIEVE_ENABLEMENT_SKILL_IDS_STAGES_CUSTOMER_IDS_FILE)
    public String[] getRetrieveEnablementSkillIdsStagesCustomerIds(final String[] csvRow) {
        return csvRow;
    }

    @TPSGeneratorDataProvider(name = "RetrieveEnabledSkillsInformationByParams",
                              inputFileType = FileType.TAB_DELIMITED,
                              fileLocation = Constants.RETRIEVE_ENABLED_SKILLS_INFORMATION_FILE)
    public String[] getRetrieveEnabledSkillsInformationByParams(final String[] csvRow) {
        return csvRow;
    }

    @TPSGeneratorDataProvider(name = "RetrieveEnablementByAsinCustomerIdsAsins",
                              inputFileType = FileType.CSV,
                              fileLocation = Constants.RETRIEVE_ENABLEMENT_BY_ASIN_CUSTOMER_IDS_ASINS_FILE)
    public String[] getRetrieveEnablementByAsinCustomerIdsAsins(final String[] csvRow) {
        return csvRow;
    }

    /**
     * Data provider method for {@code SaveSkillStage}.
     *
     * @param oneLineFromInputFile single line from the input file which
     * is list of vendorId, stage, skillTypes(separated by '$'),
     * external manifest and internal manifest in that order separated by '&'
     * @return List<String> vendorId, stage, skillTypes(separated by '$'),
     * external manifest and internal manifest in that order
     */
    @TPSGeneratorDataProvider(name = "SaveSkillStageFileReader",
                              inputFileType = FileType.TEXT,
                              fileLocation = Constants.SAVE_SKILL_STAGE_FILE)
    public List<String> getSaveSkillStageList(final String oneLineFromInputFile) {
        List<String> outputList = Arrays.asList(oneLineFromInputFile.split("&"));
        return outputList;
    }

    /**
     * Data provider method for {@code RetrieveSkillStagesByAsin}.
     *
     * @param asin skill asin
     * @return an asin
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillStagesByAsinAsins",
                              inputFileType = FileType.TEXT,
                              fileLocation = Constants.RETRIEVE_SKILL_STAGES_BY_ASIN_ASINS_FILE)
    public String retrieveSkillStagesByAsin(final String asin) {
        return asin;
    }

    /**
     * Data provider method for {@code RetrieveSkills}.
     *
     * @param csvRow - skillStageIdentifier, featureKeys
     * @return
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillsByParams",
                              inputFileType = FileType.TAB_DELIMITED,
                              fileLocation = Constants.RETRIEVE_SKILLS_FILE)
    public String[] getRetrieveSkillsByParams(final String[] csvRow) {
        return csvRow;
    }

    /**
     * This function returns list of skillstageIdentifiers from input string
     * @param input
     * @return List<SkillStageIdentifier
     */
    private List<SkillStageIdentifier> getSkillStageIdentifierList(String input) {
        List<SkillStageIdentifier> skillStageIdentifierList = new ArrayList<>();
        //remove double quotes if any
        input = input.replace("\"", "");
        //divide line in to skill stage strings
        String[] skillStageString = input.split(",");
        for(String skillStageIdentifierString : skillStageString)
        {
            String[] skillStageData = skillStageIdentifierString.split("_");
            //skillStageData[0] has skillId
            //skillStageData[1] has skill stage
            String skillId = skillStageData[0];
            String stage = skillStageData[1];
            SkillStageIdentifier skillStageIdentifier =
                    SkillStageIdentifier
                            .builder()
                            .withSkillId(skillId)
                            .withStage(stage)
                            .build();
            skillStageIdentifierList.add(skillStageIdentifier);
        }
        return skillStageIdentifierList;
    }

    /**
     * Data provider method for {@code RetrieveSkillsByAsins}.
     *
     * @param oneLineFromInputFile single line from the input file which
     * is list of asins comma separated like "[asin, asin, asin]"
     * @return List<String> List of asins
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillsByAsinsAsinList",
                              inputFileType = FileType.TEXT,
                              fileLocation = Constants.RETRIEVE_SKILLS_BY_ASINS_ASINLIST_FILE)
    public List<String> getRetrieveSkillsByAsinsAsinList(final String oneLineFromInputFile) {
        //remove double quotes if any
        String input = oneLineFromInputFile.replace("\"[", "").replace("]\"", "");
        //divide line in to asins list and return
        return new ArrayList<String>(Arrays.asList(input.split(", ")));
    }

    /**
     * Data provider method for {@code RetrieveSkillsByStatus}.
     *
     * @param oneLineFromInputFile single line from the input file which
     * is the status
     * @return String status
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillsByStatusStatusList",
                              inputFileType = FileType.TEXT,
                              fileLocation = Constants.RETRIEVE_SKILLS_BY_STATUS_STATUSLIST_FILE)
    public String getRetrieveSkillsByStatusStatusList(final String oneLineFromInputFile) {
        return oneLineFromInputFile;
    }
    
    /**
     * Data provider method for {@code RetrieveSkillsByVendor}.
     *
     * @param oneLineFromInputFile single line from the input file which
     * is vendorId
     * @return String vendorId
     */
    @TPSGeneratorDataProvider(name = "RetrieveSkillsByVendorVendorIds",
                              inputFileType = FileType.TEXT,
                              fileLocation = Constants.RETRIEVE_SKILLS_BY_VENDOR_VENDORIDS_FILE)
    public String getRetrieveSkillsByVendorVendorIds(final String oneLineFromInputFile) {
        return oneLineFromInputFile;
    }

    /**
     * Returns list of string inside quotes.
     * @param input quoted string separated by '$'.
     * @return
     */
    private List<String> getStringListFromQuotes(final String input) {
        Matcher m = Pattern.compile("\"(.*?)\"").matcher(input);
        if (m.find()) {
            return Arrays.asList(m.group(1).split("\\$"));
        } else {
            return null;
        }
    }

    /**
     * Returns list of string in string, ex: "[SMART_HOME, CUSTOM]", [CUSTOM], [*]
     *
     * @param input
     * @return
     */
    private List<String> getStringListFromRequest(String input) {
        if (StringUtils.isEmpty(input)) {
            return null;
        }
        if (input.indexOf("\"") == 0 && input.lastIndexOf("\"") == input.length()-1) {
            input = input.substring(1, input.length()-1);
        }
        if (input.indexOf("[") == 0 && input.lastIndexOf("]") == input.length()-1) {
            input = input.substring(1, input.length()-1);
        }

        return Arrays.asList(StringUtils.stripAll(input.split(",")));
    }

    /**
     * Returns list of skill stage enablement object.
     * @param skillIds list of skill id.
     * @param stages list of skill stages.
     * @return
     */
    private List<SkillStageEnablement> buildSkillStageEnablementList(List<String> skillIds, List<String> stages) {
        if (skillIds != null && stages != null) {
            List<SkillStageEnablement> enablements = new ArrayList<>();
            for (int i = 0; i< skillIds.size(); i++) {
                SkillStageEnablement skillStageEnablement = new SkillStageEnablement();
                skillStageEnablement.setSkillId(skillIds.get(i));
                skillStageEnablement.setEnabledStage(stages.get(i));
                enablements.add(skillStageEnablement);
            }
            return enablements;
        } else {
            return null;
        }
    }
}