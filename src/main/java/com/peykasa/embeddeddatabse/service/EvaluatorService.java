package com.peykasa.embeddeddatabse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.peykasa.embeddeddatabse.config.AppConfig;
import com.peykasa.embeddeddatabse.domain.model.EvState;
import com.peykasa.embeddeddatabse.domain.model.EvaluatorResult;
import com.peykasa.embeddeddatabse.domain.model.EvaluatorTask;
import com.peykasa.embeddeddatabse.domain.model.ReportModel;
import com.peykasa.embeddeddatabse.domain.repository.SqliteRepository;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author kamran
 */
@Service
public class EvaluatorService {
    private final static Logger LOGGER = LogManager.getLogger(EvaluatorService.class);
    private AppConfig config;
    private EvaluatorResult result;
    private Random random;
    private SqliteRepository repository;
    private ExecutorService executorService;

    @Autowired
    public EvaluatorService(AppConfig config, SqliteRepository repository) throws SQLException, ClassNotFoundException {
        this.repository = repository;
        this.config = config;
        result = new EvaluatorResult();
        random = new Random((new Date()).getTime());
        executorService = Executors.newFixedThreadPool(config.getConcurrentThread());

    }

    public void start() throws SQLException {
        result.setState(EvState.Running);
        Thread thread = new Thread(() -> {
            try {
                preClean();
                if (config.isInsertEnabled()) {
                    if (config.isInsertConcurrent())
                        concurrentInsert();
                    else
                        insert();
                }
                if (config.isFetchEnabled()) {
                    if (config.isFetchConcurrent())
                        concurrentFetch();
                    else
                        fetch();
                }
                if (config.isUpdateEnabled()) {
                    if (config.isUpdateConcurrent())
                        concurrentUpdate();
                    else
                        update();
                }
                if (config.isDeleteEnabled()) {
                    if (config.isDeleteConcurrent())
                        concurrentDelete();
                    else
                        delete();
                }


            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                result.setState(EvState.Done);
                executorService.shutdown();
                createOutput();
            }
        });
        thread.start();
    }

    private void createOutput() {
        ObjectMapper mapper = new ObjectMapper();
        File dir = new File(config.getOutput());
        if (!dir.exists())
            //noinspection ResultOfMethodCallIgnored
            dir.mkdir();
        try {
            mapper.writeValue(new File(dir.getAbsolutePath() + "/" + (new Date()).getTime() + ".json"), result);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void insert() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Insert);
        try {
            long count = 0;
            while (count < config.getInsertMaxRecord()) {
                ArrayList<ReportModel> batch = createBatchFake(config.getInsertBatchSize());
                count += batch.size();
                try {
                    repository.insertBulk(batch);
                    changePercent(task, count, config.getInsertMaxRecord());
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());

    }

    private void concurrentInsert() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Insert);
        try {
            final long[] count = {0};
            List<Future<?>> workers = new ArrayList<>();
            while (count[0] < config.getInsertMaxRecord()) {
                Future<?> future = executorService.submit(() -> {
                    try {
                        ArrayList<ReportModel> batch = createBatchFake(config.getInsertBatchSize());
                        repository.insertBulk(batch);
                        count[0] += batch.size();
                        changePercent(task, count[0], config.getInsertMaxRecord());
                    } catch (Exception ex) {
                        LOGGER.error(ex.getMessage(), ex);
                    }
                });
                workers.add(future);
            }
            for (Future<?> worker : workers) worker.get();

        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private void update() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Update);
        try {
            long minId = repository.getMinId();
            long count = 0;
            while (count < config.getUpdateMaxRecord()) {
                ArrayList<ReportModel> batch = createBatchFake(config.getUpdateBatchSize(), minId);
                minId += batch.size();
                count += batch.size();
                repository.updateBulk(batch);
                changePercent(task, count, config.getUpdateMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    class UpdateRunnable implements Callable<Long> {
        long minId;

        UpdateRunnable(long minId) {
            this.minId = minId;
        }

        @Override
        public Long call() throws Exception {
            ArrayList<ReportModel> batch = createBatchFake(config.getUpdateBatchSize(), minId);
            return repository.updateBulk(batch);

        }
    }

    private void concurrentUpdate() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Update);
        try {
            List<Future<Long>> workers = new ArrayList<>();
            long minId = repository.getMinId();
            long count = 0;
            while (count < config.getUpdateMaxRecord()) {
                Future<Long> worker = executorService.submit(new UpdateRunnable(minId));
                workers.add(worker);
                minId += config.getUpdateBatchSize();
                count += config.getUpdateBatchSize();

            }
            count = 0;
            for (Future<Long> worker : workers) {
                count += worker.get();
                changePercent(task, count, config.getUpdateMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private void delete() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Delete);
        try {
            long minId = repository.getMinId();
            long count = 0;
            while (count < config.getDeleteMaxRecord()) {
                long maxId = minId + config.getDeleteBatchSize();
                long result = repository.deleteBatch(minId, maxId - 1);
                minId = maxId;
                if (result > 0)
                    count += result;
                else
                    count += config.getDeleteBatchSize();
                changePercent(task, count, config.getDeleteMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private class DeleteRunnable implements Callable<Long> {
        long min;
        long max;

        DeleteRunnable(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Long call() throws Exception {
            return repository.deleteBatch(min, max);
        }
    }

    private void concurrentDelete() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Delete);
        try {
            long minId = repository.getMinId();
            long count = 0;
            ArrayList<Future<Long>> workers = new ArrayList<>();
            while (count < config.getDeleteMaxRecord()) {
                long maxId = minId + config.getDeleteBatchSize();
                Future<Long> worker = executorService.submit(new DeleteRunnable(minId, maxId - 1));
                workers.add(worker);
                minId = maxId;
                count += config.getDeleteBatchSize();
            }
            count = 0;
            for (Future<Long> worker : workers) {
                count += worker.get();
                changePercent(task, count, config.getDeleteMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private void fetch() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Fetch);
        try {
            long minId = repository.getMinId();
            LOGGER.debug("Minimum Id is :" + minId);
            long count = 0;
            while (count < config.getFetchMaxRecord()) {
                long maxId = minId + config.getFetchBatchSize();
                List<ReportModel> result = repository.fetch(minId, maxId);
                minId = maxId;
                if (result.size() > 0)
                    count += result.size();
                else
                    count += config.getFetchBatchSize();
                changePercent(task, count, config.getFetchMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private class FetchRunnable implements Callable<List<ReportModel>> {
        long min;
        long max;

        FetchRunnable(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public List<ReportModel> call() throws Exception {
            return repository.fetch(min, max);
        }
    }

    private void concurrentFetch() {
        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Fetch);
        try {
            long minId = repository.getMinId();
            long count = 0;
            List<Future<List<ReportModel>>> workers = new ArrayList<>();
            while (count < config.getFetchMaxRecord()) {
                long maxId = minId + config.getFetchBatchSize();
                Future<List<ReportModel>> future = executorService.submit(new FetchRunnable(minId, maxId));
                minId = maxId;
                workers.add(future);
                count += config.getFetchBatchSize();
            }
            count = 0;
            for (Future<List<ReportModel>> worker : workers) {
                count += worker.get().size();
                changePercent(task, count, config.getFetchMaxRecord());
            }
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

    private void changePercent(EvaluatorTask task, long number, long max) {
        double temp = Math.abs((double) number / max * 100);
        task.setPercent(temp);
        LOGGER.info(task.getType() + " Percent: " + String.format("%1$,.2f", temp) + "%");
    }

    private EvaluatorTask addTask(EvaluatorTask.TaskType type) {
        EvaluatorTask task = new EvaluatorTask(config.getResourceCheckTime().millis());
        task.setType(type);
        result.getTasks().add(task);
        task.checkResource();
        return task;
    }


    //
//    private void updateBulkData(List<ReportModel> dtos) throws Exception {
//
//        tableDao.callBatchTasks((Callable<Void>) () -> {
//            for (ReportModel dto : dtos) {
//                tableDao.update(dto);
//            }
//            return null;
//        });
////        DatabaseConnection conn = tableDao.startThreadConnection();
////        Savepoint savepoint = null;
////        try {
////            savepoint = conn.setSavePoint(null);
////            doInsert(dtos, tableDao);
////        } finally {
////            conn.commit(savepoint);
////            tableDao.endThreadConnection(conn);
////        }
//
//    }
//
//
    private void preClean() throws SQLException {
        if (!config.isClearDatabase())
            return;
        repository.clearTable();

    }

    private ArrayList<ReportModel> createBatchFake(long size) {
        ArrayList<ReportModel> batch = new ArrayList<>();
        for (long i = 0; i < size; ++i) {
            batch.add(createFake());
        }
        return batch;
    }

    private ArrayList<ReportModel> createBatchFake(long size, long startId) {
        ArrayList<ReportModel> batch = new ArrayList<>();
        for (long i = 0; i < size; ++i) {
            batch.add(createFake(startId + i));
        }
        return batch;
    }

    private ReportModel createFake(long... id) {

        ReportModel res = new ReportModel();
        if (id.length > 0)
            res.setId(id[0]);
        res.setCounter(random.nextInt());
        res.setCurrentCell(random.nextInt());
        res.setCurrentLAC(random.nextInt());
        res.setCurrentVLR(random.nextLong());
        res.setEventType(RandomStringUtils.random(10, true, true));
        return res;
    }
}
