package com.marklogic.hub.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.dataservices.ExecCaller;
import com.marklogic.client.dataservices.IOEndpoint;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.hub.HubClient;

import java.io.IOException;
import java.io.InputStreamReader;

public class JobDataManager {
    ObjectNode apiObj;
    String apiPath = "/ml-modules/root/data-hub/5/data-services/bulk/deleteJobs.api";
    ObjectMapper mapper = new ObjectMapper();
    HubClient hubClient;

    JobDataManager(HubClient hubClient) {
        this.hubClient = hubClient;
        try (InputStreamReader apiReader = new InputStreamReader(this.getClass().getResourceAsStream(apiPath))) {
            apiObj = mapper.readValue(apiReader, ObjectNode.class);
        } catch (IOException e) {
            throw new RuntimeException("Unable to find API module " + apiPath + " on classpath; cause: " + e.getMessage(), e);
        }
    }

    public void cleanJobsData(String retainDuration) {
        // See https://stackoverflow.com/a/52645128/535924 for more information about the duration regex
        if (retainDuration == null || !retainDuration.matches("^(-?)P(?=.)((\\d*)Y)?((\\d+)M)?((\\d*)D)?(T(?=\\d)(\\d+H)?(([\\d]+)M)?(([\\d]+(?:\\.\\d+)?)S)?)?$")) {
            throw new IllegalArgumentException("retainDuration must be a duration in the format of PnYnM or PnDTnHnMnS.");
        }
        DatabaseClient jobsClient = hubClient.getJobsClient();
        ObjectNode endpointConstants = mapper.createObjectNode().put("batchSize", 250).put("retainDuration", retainDuration);
        ExecCaller endpoint = ExecCaller.on(jobsClient, new JacksonHandle(apiObj));
        CapturingErrorListener errorListener = new CapturingErrorListener();

        ExecCaller.BulkExecCaller bulkCaller = endpoint.bulkCaller(endpoint.newCallContext()
                .withEndpointConstantsAs(endpointConstants));
        bulkCaller.setErrorListener(errorListener);
        bulkCaller.awaitCompletion();

        if(errorListener.didThrow()) {
            Throwable throwable = errorListener.getThrowable();
            throw new RuntimeException("Unable to delete jobs, cause: " + throwable.getMessage(), throwable);
        }
    }

    static class CapturingErrorListener implements ExecCaller.BulkExecCaller.ErrorListener {
        private Throwable throwable = null;

        boolean didThrow() {
            return this.throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

        public IOEndpoint.BulkIOEndpointCaller.ErrorDisposition processError(
                int retryCount, Throwable throwable, IOEndpoint.CallContext callContext) {
            this.throwable = throwable;
            return IOEndpoint.BulkIOEndpointCaller.ErrorDisposition.STOP_ALL_CALLS;
        }
    }
}
