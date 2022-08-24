package com.convertlab.data.runtime;

import org.apache.spark.sql.SparkSession;

public class Context {
    private SparkSession sparkSession;
    private String taskId;
    private Boolean failed;
    private String failedMessage;

    public Context(SparkSession sparkSession, String taskId) {
        this(sparkSession, taskId, false, null);
    }

    public Context(SparkSession sparkSession, String taskId, boolean failed, String failedMessage) {
        this.sparkSession = sparkSession;
        this.taskId = taskId;
        this.failed = failed;
        this.failedMessage = failedMessage;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Boolean getFailed() {
        return failed;
    }

    public void setFailed(Boolean failed) {
        this.failed = failed;
    }

    public String getFailedMessage() {
        return failedMessage;
    }

    public void setFailedMessage(String failedMessage) {
        this.failedMessage = failedMessage;
    }
}
