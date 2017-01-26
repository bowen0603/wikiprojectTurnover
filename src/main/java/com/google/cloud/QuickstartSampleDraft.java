package com.google.cloud;

// Imports the Google Cloud client library
//import com.google.api.services.bigquery.Bigquery;
//import com.google.api.services.bigquery.model.*;

import com.google.cloud.bigquery.*;

import java.io.IOException;
import java.io.PrintStream;
import java.util.stream.Collectors;

/**
 * Key steps to set up:
 * 1. Install client library: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-java
 * 2. Create the project with maven repository with needed libraries
 * 3. Install and authenticate Google Cloud SDK: gcloud beta auth application-default login
 * 4. Import proper libraries to Intellij
 *
 * Basic tasks needed:
 * 1. Execute queries, and read into a permanent table。
 * 2. Be able to wait until the end of one job, then continue another one. (https://cloud.google.com/bigquery/querying-data#asyncqueries)
 *
 *
 * Problem: two sets of APIs??? which one to use???
 * 1. com.google.cloud.bigquery
 * 2. com.google.api.services.bigquery
 */
public class QuickstartSampleDraft {
    //


    public static void main(String... args) throws Exception {
        // Instantiates a client
//        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//        BigQuery bigquery = BigQueryOptions.defaultInstance().service();

//        BigQuery bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());


        // The name for the new dataset
        String datasetName = "my_new_dataset";
        String tableName = "my_new_table";

        // Prepares a new dataset
//        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        DatasetInfo datasetInfo = DatasetInfo.builder(datasetName).build();

        // Creates the dataset
//        Dataset dataset = bigquery.create(datasetInfo);

//        System.out.printf("Dataset %s created.%n", dataset.getDatasetId().getDataset());
//        System.out.printf("Dataset %s created.%n", dataset.datasetId().dataset());

        QuickstartSampleDraft self = new QuickstartSampleDraft();
        self.runQuery("", null);
    }

    public static void run(final PrintStream out, final String queryString, final long waitTime, final boolean useLegacySql)
            throws IOException, InterruptedException {

        BigQuery bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());


        // Running synchronous queries
        QueryRequest queryRequest = QueryRequest.builder(queryString)
                                                .maxWaitTime(waitTime)
                                // Use standard SQL syntax or legacy SQL syntax for queries.
                                // See: https://cloud.google.com/bigquery/sql-reference/
                                                .useLegacySql(useLegacySql)
                                                .build();

//        QueryRequest queryRequest =
//                QueryRequest.builder(queryString)
//                        .addNamedParameter("gender", QueryParameterValue.string(gender))
//                        .addNamedParameter(
//                                "states",
//                                QueryParameterValue.array(states, String.class))
//                                // Standard SQL syntax is required for parameterized queries.
//                                // See: https://cloud.google.com/bigquery/sql-reference/
//                        .setUseLegacySql(false)
//                        .build();

        // Running asynchronous queries

        QueryResponse response = bigquery.query(queryRequest);
        // Wait for things to finish
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.jobId());
        }
        if (response.hasErrors()) {
            // handle errors
        }

        if (response.hasErrors()) {
            throw new RuntimeException(
                    response
                            .executionErrors()
                            .stream()
                            .<String>map(err -> err.message())
                            .collect(Collectors.joining("\n")));
        }

//        QueryResult result = response.getResult();
//        Iterator<List<FieldValue>> iter = result.iterateAll();
//        while (iter.hasNext()) {
//            List<FieldValue> row = iter.next();
//            out.println(row.stream().map(val -> val.toString()).collect(Collectors.joining(",")));
//        }

    }

    /**
     * example code:
     * http://programtalk.com/vs/gcloud-java/gcloud-java-bigquery/src/test/java/com/google/cloud/bigquery/it/ITBigQueryTest.java/
     */
    private void runQuery(String query, TableId destinationTable2) throws Exception {

//        TableReference tableRef = new TableReference();
//        tableRef.setProjectId("<project>");
//        tableRef.setDatasetId("<dataset>");
//        tableRef.setTableId("<name>");
//
//        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
//        queryConfig.setDestinationTable(tableRef);
//        queryConfig.setAllowLargeResults(true);
//        queryConfig.setQuery("some sql");
////        queryConfig.setCreateDisposition(CREATE_IF_NEEDED);
////        queryConfig.setWriteDisposition(WRITE_APPEND);
//
////        JobConfiguration config = new JobConfiguration();
////        JobConfiguration config = new JobConfiguration().setQuery(queryConfig);
//        QueryJobConfiguration config = new QueryJobConfiguration();
//        Job job = new Job();
//
//        job.setConfiguration(config);
//
        BigQuery bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());
//        Bigquery.Jobs.Insert insert = bigquery.jobs().insert("<projectid>", job);
////        Bigquery.Jobs.Insert insert2 = bigquery.listJobs()..insert("<projectid>", job);
//        JobReference jobId = insert.execute().getJobReference();


        // should be able to work on this one, just need to change parameters and settings.
        String tableName = "test_query_job_table";
        TableId TABLE_ID = TableId.of("bowen_user_dropouts", "bots_list");
        query = new StringBuilder()
                        .append("SELECT * FROM ")
                        .append(TABLE_ID.dataset() + "." + TABLE_ID.table())
                        .toString();
        TableId destinationTable = TableId.of("my_new_dataset", "copy_bots_list");
        QueryJobConfiguration configuration = QueryJobConfiguration.builder(query)
                .defaultDataset(DatasetId.of("my_new_dataset"))
                .destinationTable(destinationTable)
                .allowLargeResults(Boolean.TRUE)
                .flattenResults(Boolean.TRUE)
                .build();
        Job remoteJob = bigquery.create(JobInfo.of(configuration));
        remoteJob = remoteJob.waitFor();

        QueryResponse response = bigquery.getQueryResults(remoteJob.jobId());
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.jobId());
        }
    }

    private void createTable(String datasetName, String tableName) {

        BigQuery bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());
        TableId tableId = TableId.of(datasetName, tableName);
        // Table field definition
        String fieldName = "fieldName";
        Field field = Field.of(fieldName, Field.Type.string());
        // Table schema definition
        Schema schema = Schema.of(field);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.builder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
                // INTO mydataset.happyhalloween
    }
}
