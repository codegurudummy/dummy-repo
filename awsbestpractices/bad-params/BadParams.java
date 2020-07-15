package example;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;

public class BadParamsTestCases {

    public void badParams() {
        // Should flag
        new StepConfig().withName(url).withActionOnFailure("TERMINATE_JOB_FLOW");

        // Should flag
        new StepConfig().withName(url).withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW);

        // Should not flag
        new StepConfig().withName(url).withActionOnFailure(ActionOnFailure.FOO);
    }
}
