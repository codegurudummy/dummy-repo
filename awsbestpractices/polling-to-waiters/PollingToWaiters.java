package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.AssociateVpcCidrBlockRequest;
import com.amazonaws.services.ec2.model.ClassicLinkInstance;
import com.amazonaws.services.ec2.model.CreateVpcRequest;
import com.amazonaws.services.ec2.model.CreateVpcResult;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DeleteRouteRequest;
import com.amazonaws.services.ec2.model.DeleteRouteTableRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSubnetRequest;
import com.amazonaws.services.ec2.model.DeleteVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.DeleteVpcRequest;
import com.amazonaws.services.ec2.model.DescribeClassicLinkInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInternetGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeRouteTablesRequest;
import com.amazonaws.services.ec2.model.DescribeRouteTablesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.amazonaws.services.ec2.model.DetachClassicLinkVpcRequest;
import com.amazonaws.services.ec2.model.DetachInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DetachNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DhcpOptions;
import com.amazonaws.services.ec2.model.DisassociateRouteTableRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InternetGateway;
import com.amazonaws.services.ec2.model.InternetGatewayAttachment;
import com.amazonaws.services.ec2.model.NetworkAcl;
import com.amazonaws.services.ec2.model.NetworkInterface;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.Route;
import com.amazonaws.services.ec2.model.RouteTable;
import com.amazonaws.services.ec2.model.RouteTableAssociation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.Vpc;
import com.amazonaws.services.ec2.model.VpcPeeringConnection;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class PollingToVectors {
    static private Logger log = LoggerFactory.getLogger(VpcHelper.class);

    private Ec2Context cxt;

    // Needs waiter
    private void waitForTermination_needsWaiter(AmazonEC2 ec2, List<Instance> terminatedInstances) {
        long expirationTime = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        for(Instance instance: terminatedInstances) {
            while(System.currentTimeMillis() < expirationTime && ! instance.getState().getName().equals("terminated")) {
                log.info("waiting for " + instance.getInstanceId() + " " + instance.getState().getName());
                cxt.getClock().sleep(1000);
                instance = ec2.describeInstances(
                        new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId()))
                        .getReservations().get(0).getInstances().get(0);
            }
        }
    }

    // No sleep
    private void waitForTermination_noSleep(AmazonEC2 ec2, List<Instance> terminatedInstances) {
        long expirationTime = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        for(Instance instance: terminatedInstances) {
            while(System.currentTimeMillis() < expirationTime && ! instance.getState().getName().equals("terminated")) {
                log.info("waiting for " + instance.getInstanceId() + " " + instance.getState().getName());
                instance = ec2.describeInstances(
                        new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId()))
                        .getReservations().get(0).getInstances().get(0);
            }
        }
    }

    // No loop
    private void waitForTermination_noLoop(AmazonEC2 ec2, List<Instance> terminatedInstances) {
        ec2.describeInstances(
                new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId()))
                .getReservations().get(0).getInstances().get(0);
        cxt.getClock().sleep(1000);
    }

    // Can use waiter.
    // Based on
    // https://github.com/schibsted/strongbox/blob/fde1861ee83050e8cf82218f212f00ecfa17a003/sdk/src/main/java/com/schibsted/security/strongbox/sdk/internal/kv4j/generic/backend/dynamodb/GenericDynamoDB.java#L130
    private void waitForTableToBecomeActive(AmazonDynamoDB client) {
        int retries = 0;
        String tableStatus = "Unknown";
        try {
            while (retries < MAX_RETRIES) {
                log.info("Waiting for table to become active...");
                Thread.sleep(SLEEP_TIME);
                DescribeTableResult result = client.describeTable(tableName);
                tableStatus = result.getTable().getTableStatus();

                if (tableStatus.equals(TableStatus.ACTIVE.toString()) ||
                        tableStatus.equals(TableStatus.UPDATING.toString())) {
                    return;
                }

                if (tableStatus.equals(TableStatus.DELETING.toString())) {
                    throw new UnexpectedStateException(
                            tableName, tableStatus, TableStatus.ACTIVE.toString(),
                            "Table state changed to 'DELETING' before creation was confirmed");
                }
                retries++;
            }
        } catch (InterruptedException e) {
            throw new UnexpectedStateException(tableName, tableStatus, TableStatus.ACTIVE.toString(),
                    "Error occurred while waiting for DynamoDB table", e);
        }
        throw new UnexpectedStateException(tableName, tableStatus, TableStatus.ACTIVE.toString(),
                "DynamoDB table did not become active before timeout");
    }

    // Can use waiter.
    // Based on
    // https://github.com/xebialabs/overcast/blob/716e57b66870c9afe51edc3b6045863a34fb0061/src/main/java/com/xebialabs/overcast/host/Ec2CloudHost.java#L176
    // A little controvertial because it does not only wait but also returns value.
    public String waitUntilRunningAndGetPublicDnsName(AmazonEC2Client ec2) {
        // Give Amazon some time to settle before we ask it for information
        sleep(5);

        for (; ; ) {
            DescribeInstancesRequest describe = new DescribeInstancesRequest().withInstanceIds(asList(instanceId));
            Instance instance = ec2.describeInstances(describe).getReservations().get(0).getInstances().get(0);
            if (instance.getState().getName().equals("running")) {
                return instance.getPublicDnsName();
            }

            logger.info("Instance {} is still {}. Waiting...", instanceId, instance.getState().getName());
            sleep(1);
        }
    }
}
