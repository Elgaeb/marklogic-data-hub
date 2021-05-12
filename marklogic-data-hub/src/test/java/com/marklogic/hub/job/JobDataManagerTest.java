package com.marklogic.hub.job;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.hub.AbstractHubCoreTest;
import com.marklogic.hub.DatabaseKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.marklogic.client.io.DocumentMetadataHandle.Capability.*;
import static com.marklogic.client.io.DocumentMetadataHandle.Capability.EXECUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JobDataManagerTest extends AbstractHubCoreTest {

    @BeforeEach
    public void setup(){
        runAsAdmin();
        clearDatabase(getHubClient().getStagingClient());
        clearDatabase(getHubClient().getFinalClient());
        clearDatabase(getHubClient().getJobsClient());

        DocumentMetadataHandle jobMeta = new DocumentMetadataHandle();
        jobMeta.getCollections().add("Jobs");
        jobMeta.getCollections().add("Job");
        jobMeta.getPermissions().add("data-hub-developer", READ, UPDATE, EXECUTE);
        installJobDoc("/jobs/1442529761390935690.json", jobMeta, "job-test/job1.json");
        installJobDoc("/jobs/10584668255644629399.json", jobMeta, "job-test/job2.json");
        installJobDoc("/jobs/1552529761390935680.json", jobMeta, "job-test/job3.json");
        installJobDoc("/jobs/6009169633194182308.json", jobMeta, "job-test/job4.json");

        DocumentMetadataHandle batchMeta = new DocumentMetadataHandle();
        batchMeta.getCollections().add("Jobs");
        batchMeta.getCollections().add("Batch");
        batchMeta.getPermissions().add("data-hub-developer", READ, UPDATE, EXECUTE);
        installJobDoc("/jobs/batches/11368953415268525918.json", batchMeta, "job-test/batch1.json");
        installJobDoc("/jobs/batches/11345653515268525918.json", batchMeta, "job-test/batch2.json");
        installJobDoc("/jobs/batches/9346723557690375906.json", batchMeta, "job-test/batch3.json");

        assertEquals(7, getDocCount(getHubConfig().getDbName(DatabaseKind.JOB), null));
    }

    @Test
    public void clearJobData() {
        new JobDataManager(getHubClient()).cleanJobsData("PT0S");
        assertEquals(5, getDocCount(getHubConfig().getDbName(DatabaseKind.JOB), null), "5 job documents should be left due to timeEnded being N/A.");
    }

    @Test
    public void invalidArgument() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JobDataManager(getHubClient()).cleanJobsData("Bogus duration");
        }, "Should throw IllegalArgumentException when an invalid duration is provided.");
    }

}
