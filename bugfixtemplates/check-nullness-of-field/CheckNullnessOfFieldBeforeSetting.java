package bugfixtemplates;

import java.io.File;
import java.util.Objects;

class CheckNullnessOfFieldBeforeSetting {

    void m1() {
        if (firstType != null) {
            firstType = dataType;
        }
    }

    void m2() {
        if (firstType == null) {
            firstType = dataType;
        }
    }

    void m3() {
        if (Objects.nonNull(firstType)) {
            firstType = dataType;
        }
    }

    void m4() {
        if (Objects.isNull(firstType)) {
            firstType = dataType;
        }
    }

    void m5() {
        if (firstType != null) {
        } else {
            firstType = dataType;
        }
    }

    void m6() {
        if (firstType == null) {
        } else {
            firstType = dataType;
        }
    }

    void m7() {
        if (Objects.nonNull(firstType)) {
        } else {
            firstType = dataType;
        }
    }

    void m8() {
        if (Objects.isNull(firstType)) {
        } else {
            firstType = dataType;
        }
    }

    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11721716/1
    public static void setPresentPlans(final PlanModel planModel,
            final String subscriptionOverride, final String uri) {
        PlanId planId, rollOverPlanId;
        boolean eligibleForTrial = planModel.isEligibleForTrial();
        PlanId primaryOptionId = planModel.getEligiblePaidOptionsMap()
                .get(getPlanCategoryBasedOnUri(uri))
                .get(PlanDuration.MONTHLY)
                .getPlanId();
        PlanId validatedSubscriptionOverride = validateSubscriptionOverride(subscriptionOverride, planModel);
        if (null != validatedSubscriptionOverride
                && validatedSubscriptionOverride.isStudent()
                && !validatedSubscriptionOverride.getCategory().equals(getPlanCategoryBasedOnUri(uri))) {
            validatedSubscriptionOverride = primaryOptionId;
        }
    }

    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11645169/1
    private void createXPathMethodAdapters(MethodAdapterGroup currentWSDL, String currentVersion,
            Map<String, String[]> inputElementsToDisallow,
            Map<String, String[]> outputElementsToDisallow,
            Map<String, String[][]> elementsToRename) {
        if (methodAdapterToAdd != null) {
            filteringMethodAdapter = (XpathForwardingMethodAdapter) methodAdapterToAdd;
        } else {
            filteringMethodAdapter = new XpathForwardingMethodAdapter(
                    currentVersion, currentWSDL.getForwardMethodAdapterGroup().getMethodAdapter(methodName),
                    NAMESPACE_PREFIX);
            methodAdapterToAdd = filteringMethodAdapter;
        }
    }

    void m9() {
        firstType = dataType;
    }

    void m10() {
        while (firstType != null) {
            firstType = dataType;
        }
    }

}
