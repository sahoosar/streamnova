package com.di.streamnova.sql;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * SQL queries loaded from sql-queries.yml (streamnova.sql.*).
 * No SQL is hardcoded in JDBC store classes; they use these named queries.
 */
@Component
@ConfigurationProperties(prefix = "streamnova.sql")
public class SqlQueriesProperties {

    private Audit audit = new Audit();
    private Profile profile = new Profile();
    private Metrics metrics = new Metrics();

    public Audit getAudit() { return audit; }
    public void setAudit(Audit audit) { this.audit = audit; }
    public Profile getProfile() { return profile; }
    public void setProfile(Profile profile) { this.profile = profile; }
    public Metrics getMetrics() { return metrics; }
    public void setMetrics(Metrics metrics) { this.metrics = metrics; }

    public static class Audit {
        private String insert;
        private String findRecentByCaller;
        public String getInsert() { return insert; }
        public void setInsert(String insert) { this.insert = insert; }
        public String getFindRecentByCaller() { return findRecentByCaller; }
        public void setFindRecentByCaller(String findRecentByCaller) { this.findRecentByCaller = findRecentByCaller; }
    }

    public static class Profile {
        private String insertRunMeta;
        private String insertTableProfile;
        private String insertThroughput;
        private String findRunMetaByRunId;
        private String findTableProfileByRunId;
        private String findThroughputByRunId;
        private String findRunIdsByTable;
        public String getInsertRunMeta() { return insertRunMeta; }
        public void setInsertRunMeta(String insertRunMeta) { this.insertRunMeta = insertRunMeta; }
        public String getInsertTableProfile() { return insertTableProfile; }
        public void setInsertTableProfile(String insertTableProfile) { this.insertTableProfile = insertTableProfile; }
        public String getInsertThroughput() { return insertThroughput; }
        public void setInsertThroughput(String insertThroughput) { this.insertThroughput = insertThroughput; }
        public String getFindRunMetaByRunId() { return findRunMetaByRunId; }
        public void setFindRunMetaByRunId(String findRunMetaByRunId) { this.findRunMetaByRunId = findRunMetaByRunId; }
        public String getFindTableProfileByRunId() { return findTableProfileByRunId; }
        public void setFindTableProfileByRunId(String findTableProfileByRunId) { this.findTableProfileByRunId = findTableProfileByRunId; }
        public String getFindThroughputByRunId() { return findThroughputByRunId; }
        public void setFindThroughputByRunId(String findThroughputByRunId) { this.findThroughputByRunId = findThroughputByRunId; }
        public String getFindRunIdsByTable() { return findRunIdsByTable; }
        public void setFindRunIdsByTable(String findRunIdsByTable) { this.findRunIdsByTable = findRunIdsByTable; }
    }

    public static class Metrics {
        private String insertExecutionStatus;
        private String updateExecutionStatus;
        private String findExecutionByRunId;
        private String findRecentExecutions;
        private String findExecutionsByStatus;
        private String findExecutionsByCallerAgentId;
        private String insertEstimateVsActual;
        private String findEstimatesVsActualsByRunId;
        private String estimatesVsActualsBase;
        private String estimatesVsActualsFilterSource;
        private String estimatesVsActualsFilterSchema;
        private String estimatesVsActualsFilterTable;
        private String estimatesVsActualsOrderLimit;
        private String insertThroughputProfile;
        private String findThroughputByRunId;
        private String throughputBase;
        private String throughputFilterSource;
        private String throughputFilterSchema;
        private String throughputFilterTable;
        private String throughputOrderLimit;

        public String getInsertExecutionStatus() { return insertExecutionStatus; }
        public void setInsertExecutionStatus(String insertExecutionStatus) { this.insertExecutionStatus = insertExecutionStatus; }
        public String getUpdateExecutionStatus() { return updateExecutionStatus; }
        public void setUpdateExecutionStatus(String updateExecutionStatus) { this.updateExecutionStatus = updateExecutionStatus; }
        public String getFindExecutionByRunId() { return findExecutionByRunId; }
        public void setFindExecutionByRunId(String findExecutionByRunId) { this.findExecutionByRunId = findExecutionByRunId; }
        public String getFindRecentExecutions() { return findRecentExecutions; }
        public void setFindRecentExecutions(String findRecentExecutions) { this.findRecentExecutions = findRecentExecutions; }
        public String getFindExecutionsByStatus() { return findExecutionsByStatus; }
        public void setFindExecutionsByStatus(String findExecutionsByStatus) { this.findExecutionsByStatus = findExecutionsByStatus; }
        public String getFindExecutionsByCallerAgentId() { return findExecutionsByCallerAgentId; }
        public void setFindExecutionsByCallerAgentId(String findExecutionsByCallerAgentId) { this.findExecutionsByCallerAgentId = findExecutionsByCallerAgentId; }
        public String getInsertEstimateVsActual() { return insertEstimateVsActual; }
        public void setInsertEstimateVsActual(String insertEstimateVsActual) { this.insertEstimateVsActual = insertEstimateVsActual; }
        public String getFindEstimatesVsActualsByRunId() { return findEstimatesVsActualsByRunId; }
        public void setFindEstimatesVsActualsByRunId(String findEstimatesVsActualsByRunId) { this.findEstimatesVsActualsByRunId = findEstimatesVsActualsByRunId; }
        public String getEstimatesVsActualsBase() { return estimatesVsActualsBase; }
        public void setEstimatesVsActualsBase(String estimatesVsActualsBase) { this.estimatesVsActualsBase = estimatesVsActualsBase; }
        public String getEstimatesVsActualsFilterSource() { return estimatesVsActualsFilterSource; }
        public void setEstimatesVsActualsFilterSource(String estimatesVsActualsFilterSource) { this.estimatesVsActualsFilterSource = estimatesVsActualsFilterSource; }
        public String getEstimatesVsActualsFilterSchema() { return estimatesVsActualsFilterSchema; }
        public void setEstimatesVsActualsFilterSchema(String estimatesVsActualsFilterSchema) { this.estimatesVsActualsFilterSchema = estimatesVsActualsFilterSchema; }
        public String getEstimatesVsActualsFilterTable() { return estimatesVsActualsFilterTable; }
        public void setEstimatesVsActualsFilterTable(String estimatesVsActualsFilterTable) { this.estimatesVsActualsFilterTable = estimatesVsActualsFilterTable; }
        public String getEstimatesVsActualsOrderLimit() { return estimatesVsActualsOrderLimit; }
        public void setEstimatesVsActualsOrderLimit(String estimatesVsActualsOrderLimit) { this.estimatesVsActualsOrderLimit = estimatesVsActualsOrderLimit; }
        public String getInsertThroughputProfile() { return insertThroughputProfile; }
        public void setInsertThroughputProfile(String insertThroughputProfile) { this.insertThroughputProfile = insertThroughputProfile; }
        public String getFindThroughputByRunId() { return findThroughputByRunId; }
        public void setFindThroughputByRunId(String findThroughputByRunId) { this.findThroughputByRunId = findThroughputByRunId; }
        public String getThroughputBase() { return throughputBase; }
        public void setThroughputBase(String throughputBase) { this.throughputBase = throughputBase; }
        public String getThroughputFilterSource() { return throughputFilterSource; }
        public void setThroughputFilterSource(String throughputFilterSource) { this.throughputFilterSource = throughputFilterSource; }
        public String getThroughputFilterSchema() { return throughputFilterSchema; }
        public void setThroughputFilterSchema(String throughputFilterSchema) { this.throughputFilterSchema = throughputFilterSchema; }
        public String getThroughputFilterTable() { return throughputFilterTable; }
        public void setThroughputFilterTable(String throughputFilterTable) { this.throughputFilterTable = throughputFilterTable; }
        public String getThroughputOrderLimit() { return throughputOrderLimit; }
        public void setThroughputOrderLimit(String throughputOrderLimit) { this.throughputOrderLimit = throughputOrderLimit; }
    }
}
