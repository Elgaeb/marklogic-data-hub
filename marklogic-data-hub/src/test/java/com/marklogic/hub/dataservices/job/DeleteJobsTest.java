package com.marklogic.hub.dataservices.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.dataservices.ExecCaller;
import com.marklogic.client.dataservices.IOEndpoint;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.hub.AbstractHubCoreTest;
import com.marklogic.hub.dataservices.JobService;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.step.RunStepResponse;
import com.marklogic.hub.test.Customer;
import com.marklogic.hub.test.ReferenceModelProject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class DeleteJobsTest extends AbstractHubCoreTest {
    protected JobService service;
    protected ObjectMapper mapper = new ObjectMapper();

    protected ObjectNode apiObj;
    protected static String apiPath = "/ml-modules/root/data-hub/5/data-services/bulk/deleteJobs.api";

    static class CapturingErrorListener implements ExecCaller.BulkExecCaller.ErrorListener {
        private Throwable throwable = null;

        public boolean didThrow() {
            return this.throwable != null;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        public IOEndpoint.BulkIOEndpointCaller.ErrorDisposition processError(
            int retryCount, Throwable throwable, IOEndpoint.CallContext callContext) {
            this.throwable = throwable;
            return IOEndpoint.BulkIOEndpointCaller.ErrorDisposition.STOP_ALL_CALLS;
        }
    }

    @BeforeEach
     void beforeEach() throws IOException {
        Reader apiReader = new InputStreamReader(this.getClass().getResourceAsStream(apiPath));
        apiObj = mapper.readValue(apiReader, ObjectNode.class);
        setupAndRunFlow();
    }

    protected void setupAndRunFlow() {
        installProjectInFolder("test-projects/simple-custom-step");
        new ReferenceModelProject(getHubClient()).createCustomerInstance(new Customer(1, "Jane"), "staging");
        FlowInputs flowInputs = new FlowInputs();
        flowInputs.setFlowName("simpleCustomStepFlow");
        flowInputs.setSteps(Arrays.asList("1"));
        runSuccessfulFlow(flowInputs);
    }

    protected void deleteJobs(String duration) throws Throwable {
        final DatabaseClient dbClient = getHubClient().getJobsClient();
        final CapturingErrorListener errorListener = new CapturingErrorListener();

        String endpointConstants = "{\"batchSize\":1,\"retainDuration\":\"" + duration +  "\"}";
        ExecCaller endpoint = ExecCaller.on(dbClient, new JacksonHandle(apiObj));
        ExecCaller.BulkExecCaller bulkCaller = endpoint.bulkCaller(endpoint.newCallContext()
            .withEndpointConstantsAs(endpointConstants));
        bulkCaller.setErrorListener(errorListener);
        bulkCaller.awaitCompletion();

        if(errorListener.didThrow()) {
            throw errorListener.getThrowable();
        }
    }

    @Test
    void testDeleteJobs() throws Throwable {
        runAsDataHubDeveloper();
        deleteJobs("PT0S");
        int count = getJobsDocCount();
        assertEquals(0, count, "Jobs database should have no job or batch documents");
    }

    @Test
    void testDeleteJobsWithRetain() throws Throwable {
        runAsDataHubDeveloper();
        deleteJobs("P1D");
        int count = getJobsDocCount();
        assertEquals(2, count, "Jobs database should have 2 job and batch documents");
    }

    @Test
    void testAsUserWhoCannotDeleteJobs() throws Throwable {
        runAsDataHubOperator();
        try {
            deleteJobs("PT0S");
            fail("This should have failed because a data-hub-operator does not have the privileges for deleting jobs");
        } catch (Throwable e) {
            if(e instanceof FailedRequestException) {
                FailedRequestException ex = (FailedRequestException) e;
                assertEquals(HttpStatus.FORBIDDEN.value(), ex.getServerStatusCode(), "A 403 should have been returned because the user " +
                    "does not have a privilege that allows for deleting jobs");
            } else {
                throw e;
            }
        }
    }
}
