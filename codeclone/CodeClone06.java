package com.amazon.cosmodrome.control.service;

import javax.inject.Inject;

import com.amazon.cosmodrome.control.exception.DependencyException;
import com.amazon.cosmodrome.control.exception.InvalidParameterException;
import com.amazon.cosmodrome.control.util.AwsExceptionHelper;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.mturk.MTurkClient;
import software.amazon.awssdk.services.mturk.model.AssociateQualificationWithWorkerRequest;
import software.amazon.awssdk.services.mturk.model.DisassociateQualificationFromWorkerRequest;

/**
 * MTurk Gateway (client) used by the WorkFeedbackManager to blacklist workers using quals.
 */
public class MTurkGateway {
    private static final String INVALID_WORKER_ID_EXCEPTION_MESSAGE = "Could not find a worker with this identifier.";

    private final MTurkClient mTurkClient;

    @Inject
    public MTurkGateway(MTurkClient mTurkClient) {
        this.mTurkClient = mTurkClient;
    }

    /**
     * Associates a worker to a qual type in MTurk. If the worker is already associated with the qual type then this
     * operation will effectively do nothing and return successfully.
     *
     * @param workerId The worker ID as understood by MTurk.
     * @param qualTypeId The qualType ID to associate the worker to.
     * @param value An integer value that is stored with the qualification association, this value can be used by
     *              qualification requirements for additional filtering.
     * @throws InvalidParameterException If the worker ID is not recognized by MTurk.
     */
    public void associateQualificationWithWorker(String workerId, String qualTypeId, int value)
            throws InvalidParameterException {
        AssociateQualificationWithWorkerRequest request = AssociateQualificationWithWorkerRequest.builder()
                .workerId(workerId)
                .qualificationTypeId(qualTypeId)
                .integerValue(value)
                .sendNotification(false)
                .build();

        try {
            mTurkClient.associateQualificationWithWorker(request);
        } catch (SdkException e) {
            AwsServiceException ase = null;
            String errorCode = null;
            String errorMessage = null;

            if (e instanceof AwsServiceException) {
                ase = (AwsServiceException) e;
                AwsErrorDetails errorDetails = ase.awsErrorDetails();

                if (errorDetails != null) {
                    errorCode =  errorDetails.errorCode();
                    errorMessage = errorDetails.errorMessage();
                }
            }
            // This is the exception MTurk throws if the workerId is not recognized
            if ("ParameterValidationError".equals(errorCode)
                && errorMessage != null
                && errorMessage.contains("WorkerId")) {
                throwExceptionForInvalidWorkerId(ase);
            }

            // This is the exception MTurk throws if the workerId does not match a valid pattern. For our service
            // we treat this as the same exception for an unrecognized worker Id.
            if ("ValidationException".equals(errorCode)
                && errorMessage != null
                && errorMessage.contains("workerId'")) {
                throwExceptionForInvalidWorkerId(ase);
            }
            if (AwsExceptionHelper.isExceptionRetriable(e)) {
                throw new DependencyException(
                        String.format("Encountered retryable exception calling mturk:associateQualificationWithWorker"
                                        + "with qualificationTypeId=%s, workerId=%s, integerValue=%d",
                                qualTypeId, workerId, value), e);
            } else {
                throw new RuntimeException(
                        String.format("Encountered unexpected exception calling "
                                + "mturk:associateQualificationWithWorker with qualificationTypeId=%s, "
                                + "workerId=%s, integerValue=%d", qualTypeId, workerId, value), e);
            }
        }
    }

    /**
     * Disassociates a worker from a qual type in MTurk. If the worker is not already associated with the qual type then
     * this operation will effectively do nothing and return successfully.
     *
     * @param workerId The worker ID as understood by MTurk.
     * @param qualTypeId The qualType ID to disassociate the worker from.
     * @throws InvalidParameterException If the worker ID is not recognized by MTurk.
     */
    public void disassociateQualificationFromWorker(String workerId, String qualTypeId)
            throws InvalidParameterException {
        DisassociateQualificationFromWorkerRequest request =
            DisassociateQualificationFromWorkerRequest.builder()
                                                      .workerId(workerId)
                                                      .qualificationTypeId(qualTypeId)
                                                      .build();

        try {
            mTurkClient.disassociateQualificationFromWorker(request);
        } catch (SdkException e) {
            AwsServiceException ase = null;
            String errorCode = null;
            String errorMessage = null;

            if (e instanceof AwsServiceException) {
                ase = (AwsServiceException) e;
                AwsErrorDetails errorDetails = ase.awsErrorDetails();

                if (errorDetails != null) {
                    errorCode = errorDetails.errorCode();
                    errorMessage = errorDetails.errorMessage();
                }
            }

            if ("RequestError".equals(errorCode) && errorMessage != null) {

                // This is the exception MTurk throws if the workerId is not recognized
                if (errorMessage.contains("does not have qualification")) {
                    throwExceptionForInvalidWorkerId(ase);
                }

                // This is the exception MTurk throws if the workerId is valid but not already associated with
                // the qual type. In this situation we want to swallow the exception and behave like it is an
                // idempotent operation.
                if (errorMessage.contains("This operation can be called with a status of: "
                                          + "Granted")) {
                    return;
                }
            }

            // This is the exception MTurk throws if the workerId does not match a valid pattern. For our service
            // we treat this as the same exception for an unrecognized worker Id.
            if ("ValidationException".equals(errorCode)
                && errorMessage != null
                && errorMessage.contains("workerId'")) {
                throwExceptionForInvalidWorkerId(ase);
            }
            if (AwsExceptionHelper.isExceptionRetriable(e)) {
                throw new DependencyException(
                        String.format("Encountered retryable exception calling "
                                      + "mturk:disassociateQualificationFromWorker "
                                      + "with qualificationTypeId=%s, workerId=%s", qualTypeId, workerId), e);
            } else {
                throw new RuntimeException(
                        String.format("Encountered unexpected exception calling "
                                      + "mturk:disassociateQualificationFromWorker with qualificationTypeId=%s, "
                                      + "workerId=%s", qualTypeId, workerId), e);
            }
        }
    }

    private static void throwExceptionForInvalidWorkerId(Throwable cause) throws InvalidParameterException {
        throw new InvalidParameterException(cause, "workerId", INVALID_WORKER_ID_EXCEPTION_MESSAGE);
    }
}