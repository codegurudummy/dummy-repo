package resourceleak;


import com.amazon.rarsenea.GlobalVar;
import com.amazon.rarsenea.otherpackage.transform3.Transformer;
import com.amazon.rarsenea.otherpackage.transform3.Transformer.Action;
import com.amazon.rarsenea.v2.SharedSession;
import com.amazon.rarsenea.wayne.RequestTrain;
import com.amazon.rarsenea.wayne.infograb2.InfoGrab2;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.Collectors;


public class TestTooManyIfStmt {

    @Override
    protected void testTooManyIfStmt(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final RequestTrain tootoot)
            throws ServletException, IOException, JSONException {
        // TODO: blocked live servers
        final boolean tttest; // tootoot.isTest();
        if ("Alpha".equals(this.getInitParameter("NavMenu-Stage"))) {
            tttest = true;
        } else {
            tttest = tootoot.isTest();
        }

        final JSONObject replyObject = new JSONObject();
        final JSONArray errorMessages = new JSONArray();
        final JSONArray warnMessages = new JSONArray();
        final JSONArray debugMessages = new JSONArray();

        // replyObject.put("ImAlive", "POST");

        Workflows thisWorkflow = Workflows.error;
        String rackasset = null;
        String thisaction = null;
        // Object wizId = null;
        double expectedSec = 0;
        final String username = tootoot.getPilot().username;
        Long workflowId = 0L;

        JSONObject postData = null;
        try {
            final String out = request.getReader().lines().collect(Collectors.joining());
            postData = new JSONObject(out);
            debugMessages.put("POSTdata : " + postData.toString(1));

            if (!postData.isNull("RACK_ASSET")) {
                rackasset = postData.getString("RACK_ASSET").toUpperCase().trim();
            }

            final String workflow = postData.getString("WORKFLOW");
            thisaction = postData.getString("ACTION");
            // wizId = postData.get("WIZ_ID");
            // "ACTION": "START",
            // "WIZ_ID": "ID123456",
            // "RACK_ASSET": "1165600173",
            // "VERSION": "1",
            // "WORKFLOW": "DECOM_ISOLATE"

            for (final Workflows test : Workflows.values()) {
                if (test.name().equals(workflow.toUpperCase().trim())) {
                    thisWorkflow = test;
                    break;
                }

                if (test.name().equals("DECOM_" + workflow.toUpperCase().trim())) {
                    thisWorkflow = test;
                    break;
                }

            }

            if (username == null) {
                errorMessages.put("Unknown User");
            } else {
                postData.put("CONTROLLER", username);
            }

        } catch (final Exception err) {
            errorMessages.put("POSTdata Error " + err.getMessage());
        }

        if (request.getHeader("origin") != null) {
            // https://mrwiz-iad-alpha.corp.amazon.com
            final String temp = request.getHeader("origin");
            if (temp.endsWith(".amazon.com") // && temp.startsWith("https://mrwiz")
            ) {

                final String encod = URLEncoder.encode(temp.replace("https://", ""), StandardCharsets.UTF_8.displayName());
                debugMessages.put(temp + " ->  " + encod);

                response.setHeader("Access-Control-Allow-Origin", "https://" + encod);
                response.setHeader("Access-Control-Allow-Credentials", "true");
            } else {
                LOGGER.fatal("origin error! = " + temp);
                errorMessages.put("Origin error!  " + temp);
            }
        }

        final boolean isadmin = SharedSession.hasPermision(request, "GlobalRackDecomDEV");

        replyObject.put("DecomDEV", isadmin);
        final JSONArray halt_whys = new JSONArray();

        // provide blank workflow data to API
        replyObject.put("WORKFLOW_DATA", new JSONObject());
        // replyObject.put("WORKFLOW_DATA", JSONObject.NULL);

        try {

            if (thisWorkflow == Workflows.error) {
                errorMessages.put("Unknown_Workflow");
            } else if (rackasset == null) {
                errorMessages.put("rack asset missing");
            } else if (!SharedSession.hasPermision(request, "Tech-Decommissioner-Gordon")
                    && !SharedSession.hasPermision(request, "Trainee-Decommissioner-Gordon")) {

                final String warn =
                        "You are not a  <a href='https://w.amazon.com/bin/view/InfraOps/MLP/Rack_Decom/' target='_blank'>"
                                + "'Certified Rack Decom Tech'</a>. Contact your manager for training.";
                errorMessages.put(warn);
            }

            // checking for open workflows from user
            if (errorMessages.length() == 0) {

                final Transformer tf = GlobalVar.getTfDbDef("wayne", "U_Users_log", tttest);
                final ArrayList<Object> map = tf.getNewDataMap();
                final StringBuffer Prep = new StringBuffer("SELECT "
                        + " U_Users_log.* , "
                        + " IG_Bridge.D_Rack_Asset, "
                        + " IG_Bridge.BridgeType , "
                        + " ss.WFType_Name "
                        + " FROM wayne.U_Users_log "
                        + " join D_MasterStep ss on StepID = ss.ID "

                        + " join IG_Bridge on ss.RooT_Step_ID = IG_Bridge.MST_ID "
                        + " WHERE UserName = ? "
                        + " AND End_Time is null "
                        + " AND IG_Bridge.BridgeType = 'DECOM_WF' "
                        + " AND TIMESTAMPDIFF(HOUR,U_Users_log.Start_Time,CURRENT_TIMESTAMP()) < 8 ;");

                map.add(username);

                tf.setPrepstate(Prep.toString());
                tf.useJsonOutput(true);

                tf.executeOld(Action.SELECT);

                final JSONArray canuserdo = tf.getJsonOutput();

                debugMessages.put(" WORK in progress <pre>"
                        + canuserdo.toString(1)
                        + "</pre>");

                if (canuserdo.length() > 0 && "START".equalsIgnoreCase(thisaction)) {

                    for (int i = 0; i < canuserdo.length(); i++) {
                        final JSONObject output2 = canuserdo.getJSONObject(i);

                        final String baseUrl = request.getScheme() + "://"
                                + request.getServerName()
                                + (tttest ? "/wayne-test" : "/wayne")
                                + "/WsResetTool/" + output2.get("D_Rack_Asset") + "/" + output2.get("WFType_Name") + "/";

                        errorMessages.put(
                                username + " finish your current work session "
                                        + " or use the <a href='" + baseUrl + "'>the reset tool</a>"
                                        + " to cancel your current work session. <BR> ");

                    }

                    // https://decomdev.corp.amazon.com/wayne-test/WsResetTool/
                }

            }

            if (errorMessages.length() == 0) {

                JSONObject readWayneObject = null;

                final Wayne masterStep = new Wayne(tttest, errorMessages);
                masterStep.masterWorkSessionName = "DECOM_WF";
                masterStep.subStepWorkSessionName = thisWorkflow.name();

                masterStep.setDebugArray(debugMessages);
                masterStep.setWarnArray(warnMessages);
                final Long mymsid = masterStep.getMsIdByAsset(rackasset, "DECOM_WF");

                if (mymsid != null) {
                    masterStep.setCreateIfMissing(true);
                    readWayneObject = masterStep.load(mymsid);
                    workflowId = mymsid;

                    final JSONArray ttObject = masterStep.loadTickets(mymsid);
                    readWayneObject.put("TICKET_DATA", ttObject);

                    final JSONObject expectedTime = getExpectedTime(mymsid, tttest);
                    if (!expectedTime.isNull(thisWorkflow.name())) {
                        expectedSec = expectedTime.getDouble(thisWorkflow.name());
                    }

                    // WfAction.START

                    boolean hasHalt = false;
                    String regionCode = "UNK";
                    String siteCode = "UNK0";

                    if (!readWayneObject.isNull("masterStepData")) {
                        final JSONObject masterStepData = readWayneObject.getJSONObject("masterStepData");

                        if (!masterStepData.isNull("Status")) {
                            hasHalt = masterStepData.getString("Status").equalsIgnoreCase("HALT");
                        }

                        if (!masterStepData.isNull("SiteCode")) {
                            siteCode = masterStepData.getString("SiteCode");
                        }
                        if (!masterStepData.isNull("RegionCode")) {
                            regionCode = masterStepData.getString("RegionCode");
                        }

                        // SS.SiteCode,
                        // SS.RegionCode,
                    }

                    boolean hasStart = false;
                    boolean hasEnd = false;

                    JSONObject subStepData = null;

                    if (readWayneObject.isNull("subStepData")) {
                        errorMessages.put("subStepData null ");
                    } else {
                        subStepData = readWayneObject.getJSONObject("subStepData");

                        if (!subStepData.isNull("WorkflowData")) {

                            if (!subStepData.isNull("ID")) {
                                workflowId = subStepData.getLong("ID");
                            }

                            // workflowData = subStepData.getJSONObject("WorkflowData");

                            // hasStart = state.equalsIgnoreCase("START");
                            // hasEnd = !workflowData.isNull("END");
                            // hasHalt = !workflowData.isNull("HALT");

                        }

                        if (!subStepData.isNull("Step_Start_Time")) {
                            // warnMessages.put("Step_Start_Time " + subStepData.get("Step_Start_Time"));
                            hasStart = true;
                        }

                        if (!subStepData.isNull("Step_End_Time")) {
                            // warnMessages.put("Step_End_Time " + subStepData.get("Step_End_Time"));
                            hasEnd = true;
                        }

                        debugMessages.put("hasStart = " + hasStart
                                + "<br>hasEnd = " + hasEnd
                                + "<br>hasHalt = " + hasHalt);

                    }

                    // halt_whys

                    if ("START".equalsIgnoreCase(thisaction)) {

                        if (thisWorkflow == Workflows.DECOM_ISOLATE) {
                            halt_whys.put("undocumented hosts found! (PULL ANDON)");
                            try {
                                final StringBuffer htmlout = new StringBuffer();
                                final InfoGrab2 ig2 = new InfoGrab2(false, htmlout);
                                ig2.setRackAsset(rackasset);

                                ig2.loadRack();
                                readWayneObject.put("HostData", ig2.rackstuff);
                            } catch (final Exception err) {
                                errorMessages.put("IG2 Error " + err.getMessage());
                            }
                        }

                        final ArrayList<String> aList = new ArrayList<>();

                        replyObject.put("BLOCKED_USERNAMES", aList);

                        if (subStepData == null) {
                            errorMessages.put(" subStepData null ");
                        } else {

                            // workflowData != null &&

                            final String baseUrl = request.getScheme() + "://"
                                    + request.getServerName()
                                    + (tttest ? "/wayne-test" : "/wayne")
                                    + "/WsResetTool/" + rackasset + "/" + thisWorkflow + "/";

                            if (hasHalt) {
                                errorMessages.put("This Workflow is Halted!<br>");

                            } else if (hasStart && hasEnd) {

                                errorMessages.put("This Workflow was finished. <a href='" + baseUrl + "'>Reset tool</a>  <br>");
                            } else if (hasStart && !hasEnd) {

                                errorMessages.put("Work in Progress use <a href='" + baseUrl + "'>the Reset tool</a> <br>");
                                // https://decomdev.corp.amazon.com/wayne-test/WsResetTool/
                            }
                        }

                    } // end start

                    if ("END".equalsIgnoreCase(thisaction)) {
                        if (!hasStart) {
                            errorMessages.put("This Workflow has not started.<br>");
                        }
                        if (hasEnd) {
                            errorMessages.put("This Workflow is allready finished.<br>");
                        }

                    }

                    if ("HALT".equalsIgnoreCase(thisaction)) {
                        // "halt_why": "undocumented hosts found! (PULL ANDON)",

                    }

                    if (errorMessages.length() == 0) {

                        masterStep.writeSubStep(thisaction, postData, siteCode, regionCode);

                    } else {
                        errorMessages.put("Data not Recorded because of above errors!");
                    }

                } else {
                    errorMessages.put("Decom workflow not found for rack asset: " + rackasset);
                }

                halt_whys.put("This rack final needs to be final prevented from continuing.  (PULL ANDON)");
                halt_whys.put("Unable to finish workflow at this time. (RESET WORKFLOW) ");

                replyObject.put("WORKFLOW_DATA", readWayneObject);

            }
        } catch (final Exception err) {
            errorMessages.put("Error " + err.getMessage());
            LOGGER.fatal("wayne wiz epic error ", err);
        }

        replyObject.put("HALT_CODES", halt_whys);

        final JSONArray high_eff_whys = new JSONArray();

        high_eff_whys.put("I found a faster way.");
        high_eff_whys.put("Team size incorrect.");

        final JSONArray Low_eff_whys = new JSONArray();
        Low_eff_whys.put("Waiting for action by non-Decom team.");
        Low_eff_whys.put("Training new guy.");
        Low_eff_whys.put("Waiting on Decom 2man verify.");
        Low_eff_whys.put("Tool malfunction.");

        try {
            final JSONObject logData = new JSONObject();

            logData.put("rackasset", rackasset);
            logData.put("thisaction", thisaction);
            logData.put("thisWorkflow", thisWorkflow.toString());
            logData.put("errorMessages", errorMessages);

            final Transformer tf = GlobalVar.getTfDbDef("wayne", "D_MasterStep_log", tttest);
            tf.setPrepstate("INSERT INTO wayne.D_MasterStep_log (StepID,UserName,Data) "
                    + " VALUES (?,?,?); ");
            final ArrayList<Object> map = tf.getNewDataMap();

            map.add(workflowId);
            map.add(username);
            map.add(logData.toString());

            tf.executeOld(Action.INSERT);
            debugMessages.put("D_MasterStep_log: getAffectedRows " + tf.getAffectedRows());
            debugMessages.put("D_MasterStep_log: getExecutedQuery " + tf.getExecutedQuery());

        } catch (final Exception err) {
            errorMessages.put("D_MasterStep_log Error " + err.getMessage());
        }

        replyObject.put("EFFICIENCY_HIGH_CODES", high_eff_whys);
        replyObject.put("EFFICIENCY_LOW_CODES", Low_eff_whys);

        replyObject.put("EXPECTED_SECONDS", expectedSec);
        replyObject.put("DEBUG_MESSAGES", debugMessages);
        replyObject.put("WARN_MESSAGES", warnMessages);
        replyObject.put("ERROR_MESSAGES", errorMessages);

        response.setContentType("application/json");
        final PrintWriter putrack = response.getWriter();
        putrack.println(replyObject.toString(1));

    }

}
