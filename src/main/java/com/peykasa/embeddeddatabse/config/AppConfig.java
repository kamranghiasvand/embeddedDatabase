package com.peykasa.embeddeddatabse.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author kamran
 */
@Data
@Configuration
public class AppConfig {
    @Value("${app.evaluator.insert.batchSize:1000}")
    private int insertBatchSize;
    @Value("${app.evaluator.insert.maxRecord:10000}")
    private long insertMaxRecord;
    @Value("${app.evaluator.insert.concurrent:false}")
    private boolean insertConcurrent;
    @Value("${app.evaluator.insert.enabled:true}")
    private boolean insertEnabled;

    @Value("${app.evaluator.fetch.maxRecord:10000}")
    private long fetchMaxRecord;
    @Value("${app.evaluator.fetch.batchSize:1000}")
    private long fetchBatchSize;
    @Value("${app.evaluator.fetch.concurrent:false}")
    private boolean fetchConcurrent;
    @Value("${app.evaluator.fetch.enabled:true}")
    private boolean fetchEnabled;

    @Value("${app.evaluator.update.maxRecord:10000}")
    private long updateMaxRecord;
    @Value("${app.evaluator.update.batchSize:1000}")
    private long updateBatchSize;
    @Value("${app.evaluator.update.concurrent:false}")
    private boolean updateConcurrent;
    @Value("${app.evaluator.update.enabled:true}")
    private boolean updateEnabled;

    @Value("${app.evaluator.delete.maxRecord}")
    private long deleteMaxRecord;
    @Value("${app.evaluator.delete.batchSize}")
    private long deleteBatchSize;
    @Value("${app.evaluator.delete.concurrent:false}")
    private boolean deleteConcurrent;
    @Value("${app.evaluator.delete.enabled:true}")
    private boolean deleteEnabled;

    @Value("${app.evaluator.connectionString:jdbc:sqlite:test.db}")
    private String connectionString;
    @Value("${app.evaluator.clearDatabase:true}")
    private boolean clearDatabase;
    @Value("${app.evaluator.resourceCheckTime:1s}")
    private TimeConfig resourceCheckTime;
    @Value("${app.evaluator.output:/output/}")
    private String output;
    @Value("${app.evaluator.dbPass:}")
    private String dbPass;
    @Value("${app.evaluator.dbUser:}")
    private String dbUser;
    @Value("${app.evaluator.concurrentThreads:10}")
    private int concurrentThread;
}
