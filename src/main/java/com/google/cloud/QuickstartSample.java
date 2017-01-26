package com.google.cloud;

// Imports the Google Cloud client library
//import com.google.api.services.bigquery.Bigquery;
//import com.google.api.services.bigquery.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.sun.xml.internal.xsom.impl.scd.Iterators;
import javafx.scene.control.Tab;

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
 *
 * TODO to watch out when writing the code:
 * 1. add an extra space at the end of a statement for the query validation.
 * 2. add the month unit for the table of each table to create separate table sets.
 */
public class QuickstartSample {

    private BigQuery bigquery;
    private String defaultDataset;
    private int timeIntervalUnit; // in months


    QuickstartSample() {
        timeIntervalUnit = 1;
        defaultDataset = "bowen_quitting_script";
        this.bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());
    }


    /**
     * * Logic of analysis:
     * 1. group composition - purely based on the number of each types of members
     * ** Done : identifyMembersInProjects()
     * ************************************************************************************
     * 2. member experience - based on editor's behaviors on the previous time interval
     * key tables: (1) user-wp-tcount, (2) user-ns-edits history
     * table (1): script_user_wp_active_range_revs45
     * table (2): should be generated already, need to relocate those tables (revision tables)
     *
     *
     * ************************************************************************************
     * Key functions, tables, and operationalizations
     * *** 1. (user, wp, active range) table
     * A key table contains the information of editor's active period on each project.
     * the time interval is calculated based on the initial setting of time range.
     * Editor's active range on the project is estimated by the edits on ns 4 and 5 which is questionable still.
     * todo: Might need better rational on this?
     * table: script_user_wp_active_range_revs45
     *
     * *** 2. (wp, tcount) table
     * table: script_user_wp_revs_45_valid_users_wps_valid_range
     *
     *
     */


    public static void main(String... args) throws Exception {
        QuickstartSample self = new QuickstartSample();

        // Create the valid range of the project in the following three steps
//        self.createTimeRangeOfAllProjects();
//        self.createFullTimeIntervalFile();
//        self.createValidTimeRangeForProjects();


        // Create longitudinal data for DVs
//        self.createLongitudinalDVs();


        // Create longitudinal data for IVs
//        self.identifyMembersInProjects();

        // Merge the above variables generated into one table
//        self.mergeDVsAndMemberComposition();


        // Compute advanced member attributes
        self.computeAdvancedMemberAttributes();


        System.out.println("Done .. ");
    }

    private void computeAdvancedMemberAttributes() throws Exception {
//        distinguishProjectLeaverAttributes();
//        computeHighLowPerformanceMembers();
        mergeProjectLeaverAttributes();
    }

    private void computeHighLowPerformanceMembers() throws Exception {
        // given table: nwikiproject, tcount, mean, high_bar, low_bar

        //todo: update table names



    }

    private void mergeProjectLeaverAttributes() throws Exception {
        //TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit));
        //TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit));

        // merge high/low productive leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_prod_leavers AS nbr_high_prod_leavers," +
                        "t2.nbr_low_prod_leavers AS nbr_low_prod_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_prod_high_low_nbrs" + timeIntervalUnit));

        // merge high/low project coordination leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_coors_leavers AS nbr_high_coors_leavers," +
                        "t2.nbr_low_coors_leavers AS nbr_low_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_coors_high_low_nbrs" + timeIntervalUnit));

        // merge the two tables above
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_prod_leavers AS nbr_high_prod_leavers," +
                        "t1.nbr_low_prod_leavers AS nbr_low_prod_leavers," +
                        "t2.nbr_high_coors_leavers AS nbr_high_coors_leavers," +
                        "t2.nbr_low_coors_leavers AS nbr_low_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_merging_prod_high_low_nbrs" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_merging_coors_high_low_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_coors_prod_high_low_nbrs" + timeIntervalUnit));

        // merge into previous table
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t1.project_art_comm AS project_art_comm," +
                        "t1.project_user_comm AS project_user_comm," +
                        "IFNULL(t2.nbr_high_prod_leavers, 0) AS nbr_high_prod_leavers," +
                        "IFNULL(t2.nbr_low_prod_leavers, 0) AS nbr_low_prod_leavers," +
                        "IFNULL(t2.nbr_high_coors_leavers, 0) AS nbr_high_coors_leavers," +
                        "IFNULL(t2.nbr_low_coors_leavers, 0) AS nbr_low_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_merging_coors_prod_high_low_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm_leavers_high_low_prod_coors"+timeIntervalUnit));
    }

    /**
     * Compute high/low performance editors based on standard deviations
     * TODO: document details about IVs generations (what variable and how to generate) based on the existing query document
     * TODO: check what was done, what variables are meaningful to generate (the meaning and rationale of having it)
     */

    private void distinguishProjectLeaverAttributes() throws Exception {

        // todo newcomers - previous experience (# of projects participated before)
        /**
         * Documenting highly productive leaver:
         * For the current time interval t_i, find the productivity in the previous time interval t_(i-1),
         * compute the mean of remainings, (include leavers who were remainings at that time)
         * and the three bins. find out which bin the leavers belong to. But they are the leavers for t_i.
         */

        // TODO: check out commands: Longitudinal IV - Productivity Leavers & pct of high productive leavers
        // TODO: combine that with: Longitudinal IV - Three Bins for leavers


        // ** Work on user article productivity
        // Have the edits grouped by users, wikiproject, time intervals
        runQuery("SELECT t1.user_id AS user_id," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + tableName("bowen_wikis_quitting", "revs_ns0_encoded_valid_users", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id " +
                        "WHERE t1.timestamp < t2.tcount_end_ts AND t1.timestamp > t2.tcount_start_ts",
                TableId.of(defaultDataset, "script_ns0_user_wp_edits_records"+timeIntervalUnit));

        runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS aggr_productivity," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_edits_records" + timeIntervalUnit) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr"+timeIntervalUnit));

        // fill the missing tcounts
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.aggr_productivity) AS aggr_productivity," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount ",
                        //"ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full"+timeIntervalUnit));

        // adding the time range on each editor for each project he involved
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.aggr_productivity AS aggr_productivity," +
                        "t2.first_tcount AS first_tcount," +
                        "t2.last_tcount AS last_tcount," +
                        "t2.leaving_tcount AS leaving_tcount," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit));

        // connect to the productivity in the previous time interval
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_productivity AS cur_aggr_productivity," +
                        "t2.aggr_productivity AS pre_aggr_productivity," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        // select only remaining members, and compute the mean and stdv for each tcount of projects
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(pre_aggr_productivity) AS pre_prod_mean," +
                        "IFNULL(STDDEV(pre_aggr_productivity), 0) AS pre_prod_stdv," +
                        "COUNT(*) AS nbr_remainings," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit) +
                        "WHERE first_tcount != tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_stats"+timeIntervalUnit));

        // compute high and low bars for productivity for each wikiproject in each time interval
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "pre_prod_mean AS pre_prod_mean," +
                        "(pre_prod_mean + pre_prod_stdv) AS high_bar," +
                        "IF((pre_prod_mean - pre_prod_stdv) < 0, 0, pre_prod_mean - pre_prod_stdv) AS low_bar," +
                        "nbr_remainings AS nbr_remainings," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_stats"+timeIntervalUnit),
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit));

        // high productivity leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_prod_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t1.nbr_remainings AS nbr_remainings," +
                        "t2.pre_aggr_productivity AS pre_aggr_productivity," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_productivity > t1.high_bar",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_userids"+timeIntervalUnit));

        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_high_prod_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_high_prod_leavers) AS nbr_high_prod_leavers," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit));

        // ** low productivity leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_prod_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t2.pre_aggr_productivity AS pre_aggr_productivity," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_productivity < t1.low_bar ",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_userids"+timeIntervalUnit));

        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_low_prod_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_low_prod_leavers) AS nbr_low_prod_leavers," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit));

        //** Work on user project coordinations

        // raw edits on ns 4 and 5: lng_user_wikiproject_valid_revs_45

        // Have the edits grouped by users, wikiproject, time intervals
        runQuery("SELECT t1.user_id AS user_id," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id " +
                        "WHERE t1.timestamp < t2.tcount_end_ts AND t1.timestamp > t2.tcount_start_ts",
                TableId.of(defaultDataset, "script_ns45_user_wp_edits_records"+timeIntervalUnit));

        runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS aggr_coordination45," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_edits_records" + timeIntervalUnit) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr"+timeIntervalUnit));

        // fill the missing tcounts
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.aggr_coordination45) AS aggr_coordination45," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full"+timeIntervalUnit));

        // adding the time range on each editor for each project he involved
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.aggr_coordination45 AS aggr_coordination45," +
                        "t2.first_tcount AS first_tcount," +
                        "t2.last_tcount AS last_tcount," +
                        "t2.leaving_tcount AS leaving_tcount," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit));

        // connect to the productivity in the previous time interval
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_coordination45 AS cur_aggr_coordination45," +
                        "t2.aggr_coordination45 AS pre_aggr_coordination45," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_productivity AS cur_aggr_productivity," +
                        "t2.aggr_productivity AS pre_aggr_productivity," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        // select only remaining members, and compute the mean and stdv for each tcount of projects
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(pre_aggr_coordination45) AS pre_coors_mean," +
                        "IFNULL(STDDEV(pre_aggr_coordination45), 0) AS pre_coors_stdv," +
                        "COUNT(*) AS nbr_remainings," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit) +
                        "WHERE first_tcount != tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_stats"+timeIntervalUnit));

        // compute high and low bars for coordination for each wikiproject in each time interval
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "pre_coors_mean AS pre_coors_mean," +
                        "(pre_coors_mean + pre_coors_stdv) AS high_bar," +
                        "IF((pre_coors_mean - pre_coors_stdv) < 0, 0, pre_coors_mean - pre_coors_stdv) AS low_bar," +
                        "nbr_remainings AS nbr_remainings," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_stats"+timeIntervalUnit),
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit));

        // high coordination leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_coors_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t1.nbr_remainings AS nbr_remainings," +
                        "t2.pre_aggr_coordination45 AS pre_aggr_coordination45," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_coordination45 > t1.high_bar",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_userids"+timeIntervalUnit));

        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_high_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_high_coors_leavers) AS nbr_high_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs_full" + timeIntervalUnit));

        // ** low coordination leavers
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_coors_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t2.pre_aggr_coordination45 AS pre_aggr_coordination45," +
                        "FROM " + tableName(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_coordination45 < t1.low_bar ",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_userids"+timeIntervalUnit));

        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_low_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_low_coors_leavers) AS nbr_low_coors_leavers," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs_full" + timeIntervalUnit));

        // Newcomer's prior experience - number of WPs joined
        // Longitudinal IV - Newcomer Prior Experience by # of WPs Joined
    }

    private void computeControlVariables() {
        /*
        CV - Project tenure
Wikiproject creation timestamp
Sql
SELECT nwikiproject,
       MIN(first_edit) AS wp_creation_ts,
FROM [bowen_wikis_quitting.rev_ns45_user_wp_global_tcount_range_valid]
GROUP BY nwikiproject

Table: wikiproject_creation_ts
Wikiproject age to each time interval
Sql
SELECT t1.nwikiproject AS nwikiproject,
       t2.tcount AS tcount,
       IF(t2.start_ts - t1.wp_creation_ts > 0, t2.start_ts - t1.wp_creation_ts, 0) AS wp_tenure,
FROM [bowen_wikis_quitting.wikiproject_creation_ts] t1
JOIN [bowen_wikis_quitting.lng_historical_tcount_6months] t2
ON t1.nwikiproject = t2.nwikiproject
Table: lng_cv_project_tenure_partial
Filter out zeros and convert time unit.
Sql
SELECT t1.nwikiproject AS nwikiproject,
       t1.tcount AS tcount,
       t1.wp_tenure / (3600*24*30) AS wp_tenure,
FROM [bowen_wikis_quitting.lng_cv_project_tenure_partial] t1
WHERE wp_tenure != 0
ORDER BY nwikiproject, tcount
Table: lng_cv_project_tenure

         */
    }



    private void mergeDVsAndMemberComposition() throws Exception {

        // merge newcomers and leavers nbr
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t2.remainings_nbr AS remainings_nbr," +
                        "FROM " + tableName(defaultDataset, "script_wp_newcomers_nbr_tcount_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_wp_remainings_nbr_tcount_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_newcomers_remainings"+timeIntervalUnit));

        // merge with leavers nbr
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t2.leavers_nbr AS leavers_nbr," +
                        "FROM " + tableName(defaultDataset, "script_merging_newcomers_remainings" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_wp_leavers_nbr_tcount_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_newcomers_remainings_leavers"+timeIntervalUnit));

        // merge with dv prod
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t2.group_article_productivity AS group_article_productivity," +
                        "FROM " + tableName(defaultDataset, "script_merging_newcomers_remainings_leavers" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod"+timeIntervalUnit));

        // merge with dv coor
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t2.project_coors AS project_coors," +
                        "FROM " + tableName(defaultDataset, "script_mbr_comp_dv_prod"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "dv_wp_full_coor45_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors"+timeIntervalUnit));

        // merge with dv art_comm
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t2.project_art_comm AS project_art_comm," +
                        "FROM " + tableName(defaultDataset, "script_mbr_comp_dv_prod_coors"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm"+timeIntervalUnit));

        // merge with dv user comm
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t1.project_art_comm AS project_art_comm," +
                        "IFNULL(t2.project_user_comm, 0) AS project_user_comm," + // user comm is missing some projects
                        "FROM " + tableName(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm"+timeIntervalUnit));
    }

    /**
     * Identify the numbers of newcomers, leavers, and remaining members of each project in each time interval.
     * - create editor-wp-tcount table
     *
     */
    private void identifyMembersInProjects() throws Exception {
        // starting from table: (Find the first and last time interval for each editor on wikiprojects (6-month interval))
        // in file Queries and Tables

        // Locate the first edit to a time interval of an editor to a project
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t2.tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_range_encoded", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.first_edit AND t1.first_edit < t2.end_ts ",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45_partial"+timeIntervalUnit));

        // Locate the last edit for the editor on each project involved
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "t2.tcount AS last_tcount," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_start_end_revs45_partial" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.last_edit AND t1.last_edit < t2.end_ts",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45"+timeIntervalUnit));

        // Create the entire active range of the editor on the project by filling the gaps
        runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.start_ts AS tcount_start_ts," +
                        "t2.end_ts AS tcount_end_ts," +
                        "t2.tcount AS tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "(t1.last_tcount+1) AS leaving_tcount," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_start_end_revs45" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.first_edit < t2.end_ts AND t1.last_edit > t2.start_ts " +
                        "ORDER BY nwikiproject, user_id, tcount",
                TableId.of(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit));

        // Compute newcomers for each project in each time interval
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS newcomers_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE tcount = first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount" + timeIntervalUnit));
        // add in time intervals with no such members
        // TODO: do not count the first and last time intervals - to remove (identify the value)
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.newcomers_nbr, 0) AS newcomers_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_wp_newcomers_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO: check the value
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount_full"+timeIntervalUnit));

        // leavers
        runQuery("SELECT nwikiproject," +
                        "tcount+1 AS tcount," +
                        "COUNT(UNIQUE(user_id)) AS leavers_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE (tcount+1) = leaving_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount" + timeIntervalUnit));

        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.leavers_nbr, 0) AS leavers_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_wp_leavers_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount_full"+timeIntervalUnit));

        // remaining members
        runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS remainings_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE tcount != first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount"+timeIntervalUnit));

        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.remainings_nbr, 0) AS remainings_nbr," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "script_wp_remainings_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount_full"+timeIntervalUnit));
    }


    /**
     * Compute DVs in the each time interval for each wikiproject
     * DVs are only from valid members and valid wikiprojects
     * TODO: why user comm is high? Editors are still talking even they are no longer involved in the project.
     */
    private void createLongitudinalDVs() throws Exception {

        // DV - coordination: total number of coordination in ns 45 of each project in each time interval
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS project_coordination," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_encoded_full", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals todo: only time project level needed tcount, or users?
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                "t1.tcount AS tcount," +
                "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.project_coordination) AS project_coors," +
                "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                "LEFT JOIN " + tableName(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit, "t2") +
                "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                "ORDER BY nwikiproject, tcount", TableId.of(defaultDataset, "dv_wp_full_coor45_per_time_interval_months"+timeIntervalUnit));

        // DV -  productivity: total article edits of the members
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS group_article_productivity," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns0_member_project_edits", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.group_article_productivity) AS group_article_productivity," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // DV: article communication
        // TODO: need to check the alignment of time intervals
        // TODO: check rev_ns1_member_project_edits table - article edits??? (confusing name)
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS art_comm," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns1_member_project_edits", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.art_comm) AS project_art_comm," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // DV: user communication (using previous generated table)
        // append nwikiproject to wikiproject
        runQuery("SELECT t2.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t1.user_talk_from AS user_talk_from," +
                        "t1.former_member AS former_member," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + tableName("bowen_user_dropouts", "user_talks_to_former_wikiprojects_members", "t1") +
                        "INNER JOIN " + tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_valid_user_comm_records_encoded"));

        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS user_comm," +
                        "FROM " + tableName(defaultDataset, "script_valid_user_comm_records_encoded", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.user_comm) AS project_user_comm," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit));
    }

    private void createTimeRangeOfAllProjects() throws Exception {

        // merge ns 4 and 5
        runQuery("SELECT *" +
                        "FROM bowen_wikis_quitting.rev_ns4_user_wikiproject_ts, bowen_wikis_quitting.rev_ns5_user_wikiproject_ts",
                TableId.of(defaultDataset, "script_rev_ns45_user_wp_ts"));


        // select valid editors
        runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + defaultDataset + "." + "script_rev_ns45_user_wp_ts AS t1 " +
                        "INNER JOIN bowen_editor_attachments.users_valid_withIds AS t2 " +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users"));

        // select valid project
        runQuery("SELECT t1.user_text AS user_text," +
                        "t1.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + defaultDataset + "." + "script_user_wp_revs_45_valid_users AS t1 " +
                        "INNER JOIN bowen_editor_attachments.wikiprojects_valid_withIds AS t2 " +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users_wps"));

        // find max and min timestamps
        runQuery("SELECT MAX(timestamp) AS maxTS," +
                        "MIN(timestamp) AS minTS," +
                        "FROM " + defaultDataset + "." + "script_user_wp_revs_45_valid_users_wps",
                TableId.of(defaultDataset, "script_ts_range_revs_45"));


        /*
         *   given the max and min range of the dataset, create the range split by X months
         *   need to manually input the max and min timestamp based on the previous query result
         */
        createFullTimeIntervalFile();

        // TODO: need to manually upload the table to Bigquery

        /*
         * Need to manually upload the table created the last function before
         * running the following function.
         */
        createValidTimeRangeForProjects();
    }

    // TODO work on this, or use it as a start of another logic flow
    private void createValidTimeRangeForProjects() throws Exception {
        // select the time range of each wikiproject based on time range of ns 4 and 5
        runQuery("SELECT nwikiproject," +
                "MAX(timestamp) AS maxTS," +
                "MIN(timestamp) AS minTS," +
                "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps") +
                "GROUP BY nwikiproject", TableId.of(defaultDataset, "script_wp_revs_45_time_range"));

        // create valid project time range
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "FROM " + tableName(defaultDataset, "script_wp_revs_45_time_range", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "full_time_intervals_" + timeIntervalUnit + "month", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.minTS < t2.end_ts AND t1.maxTS > t2.start_ts",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit));

        // TODO: may want to add the starting tcount and ending tcount for each project
    }

    private void logic() {

        /**
         * 1. Compute the time range of all the projects based on ns 4 or 5.
         * 2. Split by 1 or 2 months for each project on the timeline.
         * 3. Join with editing tables of editors for the computation of the variables of each time interval
         *
         * Breakdowns:
         * 1. generate DVs first - need to create new lng_wikiproject_prod_per_time_interval_6months table for X months
         *      - DV - WikiProject Productivity Change per time interval
         */


    }

    private void createFullTimeIntervalFile() {
        int maxTS = 1433341432;
        int minTS = 1001492429, startTS = minTS;

        // idea: add in more projects 0 - 2000 for the entire range, later do inner join to remove redundant projects
        // output: nwikiproject, ts_start, ts_end, tcount
        List<Integer> timeRange = new LinkedList<Integer>();
        while (startTS < maxTS) {
            timeRange.add(startTS);
            startTS += 3600 * 24 * 30 * timeIntervalUnit;
        }
        timeRange.add(maxTS);

        try{
            PrintWriter writer = new PrintWriter(String.format("full_time_intervals%d.csv", timeIntervalUnit), "UTF-8");
            writer.printf("nwikiproject,start_ts,end_ts,tcount\n");
            for (int id = 0; id <= 2000; ++id) {
                for (int i = 0; i < timeRange.size()-1; ++i) {
                    writer.printf("%d,%d,%d,%d\n", id, timeRange.get(i), timeRange.get(i+1),(i+1));
                }
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("Having errors when writing out to the file.");
        }
    }

    private String tableName(String dataset, String table) {
        return dataset + "." + table + " ";
    }

    private String tableName(String dataset, String table, String shortCut) {
        return dataset + "." + table + " AS " + shortCut + " ";
    }

    // https://cloud.google.com/bigquery/querying-data#bigquery-sync-query-java
    private void runQuery(String query, TableId destinationTable) throws Exception {

        double startTime = System.currentTimeMillis();

        QueryJobConfiguration configuration = QueryJobConfiguration.builder(query)
                .defaultDataset(DatasetId.of(destinationTable.dataset()))
                .destinationTable(destinationTable)
                .allowLargeResults(Boolean.TRUE)
                .flattenResults(Boolean.TRUE)
                .writeDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE) // overwrite
                .build();
        Job remoteJob = bigquery.create(JobInfo.of(configuration));
        remoteJob = remoteJob.waitFor();

        QueryResponse response = bigquery.getQueryResults(remoteJob.jobId());
        while (!response.jobCompleted()) {
            Thread.sleep(10000);
            response = bigquery.getQueryResults(response.jobId());
        }

        double endTime = System.currentTimeMillis();
        double totalTime = endTime - startTime;

        if (response.hasErrors()) {
            throw new RuntimeException(
                    response
                            .executionErrors()
                            .stream()
                            .<String>map(err -> err.message())
                            .collect(Collectors.joining("\n")));
        }

        System.out.printf("Table [%s.%s] created in %f seconds.%n", destinationTable.dataset(),
                destinationTable.table(),
                totalTime/1000);
    }
}
