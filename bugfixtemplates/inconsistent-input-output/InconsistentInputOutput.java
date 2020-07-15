package bugfixtemplates;

class InconsistentInputOutput {
    
    void positive() {
        DeploymentIdentifier deploymentId = null;
        DeploymentItem deploymentItem = null;
        try {
            deploymentItem = loadDeployment(deploymentId);
            if (deploymentItem.getDeploymentStatus() == DeploymentStatus.QUEUED) {
                break;
            }
        } catch (DeploymentNotFoundException e) {
            LOGGER.warn("An invalid deployment entry was found in the queue.", e);
        }
        use(deploymentId);
        use(deploymentItem);
    }
    
    void negativeWithReset() {
        DeploymentIdentifier deploymentId = null;
        DeploymentItem deploymentItem = null;
        try {
            deploymentItem = loadDeployment(deploymentId);
            if (deploymentItem.getDeploymentStatus() == DeploymentStatus.QUEUED) {
                break;
            }
        } catch (DeploymentNotFoundException e) {
            LOGGER.warn("An invalid deployment entry was found in the queue.", e);
            deploymentId = null;
        }
        use(deploymentId);
        use(deploymentItem);
    }
    
    void negativeWithoutUseInput() {
        DeploymentIdentifier deploymentId = null;
        DeploymentItem deploymentItem = null;
        try {
            deploymentItem = loadDeployment(deploymentId);
            if (deploymentItem.getDeploymentStatus() == DeploymentStatus.QUEUED) {
                break;
            }
        } catch (DeploymentNotFoundException e) {
            LOGGER.warn("An invalid deployment entry was found in the queue.", e);
        }
        use(deploymentItem);
    }
    
    void negativeWithoutUseOutput() {
        DeploymentIdentifier deploymentId = null;
        DeploymentItem deploymentItem = null;
        try {
            deploymentItem = loadDeployment(deploymentId);
            if (deploymentItem.getDeploymentStatus() == DeploymentStatus.QUEUED) {
                break;
            }
        } catch (DeploymentNotFoundException e) {
            LOGGER.warn("An invalid deployment entry was found in the queue.", e);
        }
        use(deploymentId);
    }

}
