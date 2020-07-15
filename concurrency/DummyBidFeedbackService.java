/*
 * Copyright (c) 2010  A9.com, Inc. or its affiliates.
 * All Rights Reserved.
 *
 */

package concurrency;

import com.a9.cpx.stubutils.IntermittentFailureSimulator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class DummyBidFeedbackService {

    private AtomicLong feedBackCounter = new AtomicLong();
    private final LinkedList<String> lastRequests = new LinkedList<String>();
    private final Random random = new Random(System.currentTimeMillis());

    private int howManyRequestsToKeep = 100;
    private IntermittentFailureSimulator intermittentFailureSimulator;

    public void process (HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String cmd = req.getPathInfo();
        if (cmd != null && cmd.endsWith("/reset")) {

            synchronized (lastRequests) {
                lastRequests.clear();
            }

            feedBackCounter.set(0);

        } else if (cmd != null && cmd.endsWith("/get")) {

            resp.setContentType("text/javascript");

            JSONObject ret = new JSONObject();
            try {
                ret.put("total", feedBackCounter.get());

                String[] last;

                synchronized (lastRequests) {
                    last = lastRequests.toArray(new String[lastRequests.size()]);
                }

                ret.put("last", new JSONArray(Arrays.asList(last)));

                PrintWriter writer = resp.getWriter();
                writer.append(ret.toString(2)).append("\n");
            } catch (JSONException e) {
                throw new RuntimeException("Couldn't produce JSON: " + e, e);
            }

        } else {

            if (intermittentFailureSimulator != null) {
                try {
                    intermittentFailureSimulator.maybeFail();
                } catch (Throwable throwable) {
                    resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Too busy");
                    return;
                }
            }

            String query = req.getQueryString();

            if (howManyRequestsToKeep > 0) {
                synchronized (lastRequests) {
                    while (lastRequests.size() > howManyRequestsToKeep) {
                        lastRequests.removeFirst();
                    }
                    lastRequests.add(query);
                }
            }

            feedBackCounter.incrementAndGet();
        }
    }


    public int getHowManyRequestsToKeep() {
        return howManyRequestsToKeep;
    }

    public void setHowManyRequestsToKeep(int howManyRequestsToKeep) {
        this.howManyRequestsToKeep = howManyRequestsToKeep;
    }

    public IntermittentFailureSimulator getIntermittentFailureSimulator() {
        return intermittentFailureSimulator;
    }

    public void setIntermittentFailureSimulator(IntermittentFailureSimulator intermittentFailureSimulator) {
        this.intermittentFailureSimulator = intermittentFailureSimulator;
    }
}