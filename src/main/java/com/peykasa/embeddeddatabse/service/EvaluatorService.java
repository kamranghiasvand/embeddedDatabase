package com.peykasa.embeddeddatabse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.peykasa.embeddeddatabse.config.AppConfig;
import com.peykasa.embeddeddatabse.domain.model.EvState;
import com.peykasa.embeddeddatabse.domain.model.EvaluatorResult;
import com.peykasa.embeddeddatabse.domain.model.EvaluatorTask;
import com.peykasa.embeddeddatabse.domain.model.ReportModel;
import com.peykasa.embeddeddatabse.domain.repository.SqliteRepository;
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
//    private Dao<ReportModel, Long> tableDao;
  //  private ConnectionSource source;
    private EvaluatorResult result;
    private Random random;
    private SqliteRepository repository;
    ExecutorService executorService ;

    @Autowired
    public EvaluatorService(AppConfig config, SqliteRepository repository) throws SQLException, ClassNotFoundException {
        this.repository = repository;
        this.config = config;
        result = new EvaluatorResult();
        random = new Random((new Date()).getTime());
        executorService   = Executors.newFixedThreadPool(config.getConcurrentThread());

    }

    public void start() throws SQLException {
        result.setState(EvState.Running);
        Thread thread = new Thread(() -> {
            try {
       //         preClean();
                if (config.isInsertEnabled()) {
                    if (config.isInsertConcurrent())
                        concurrentInsert();
                    else
                        insert();
                }
//                if (config.isFetchEnabled()) {
//                    if (config.isFetchConcurrent())
//                        concurrentFetch();
//                    else
//                        fetch();
//                }
//                if (config.isUpdateEnabled()) {
//                    if (config.isUpdateConcurrent())
//                        concurrentUpdate();
//                    else
//                        update();
//                }
//                if (config.isDeleteEnabled()) {
//                    if (config.isDeleteConcurrent())
//                        concurrentDelete();
//                    else
//                        delete();
//                }


            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                result.setState(EvState.Done);
                createOutput();
            }
        });
        thread.start();
    }

    private void createOutput() {
        ObjectMapper mapper = new ObjectMapper();
        File dir = new File(config.getOutput());
        if (!dir.exists())
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
                    changePercent(task, count, config.getInsertMaxRecord());
                    insertBulkData(batch);
                    LOGGER.debug("total insert: " + count);
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
            long count = 0;
            List<Future<?>> workers = new ArrayList<>();
            while (count < config.getInsertMaxRecord()) {

                Future<?> future=executorService.submit(() -> {
                    try {
                        ArrayList<ReportModel> batch = createBatchFake(config.getInsertBatchSize());
                        insertBulkData(batch);
                    } catch (Exception ex) {
                        LOGGER.error(ex.getMessage(), ex);
                    }
                });
                workers.add(future);
                count += config.getInsertBatchSize();
                changePercent(task, count, config.getInsertMaxRecord());
            }
            for (Future<?> worker : workers) worker.get();
        } catch (Exception ex) {
            task.setException(ex);
            LOGGER.error(ex.getMessage(), ex);
        }
        task.setEndNanoTime(System.nanoTime());
    }

//    private void update() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Update);
//        try {
//            long minId = getMinId();
//            long count = 0;
//            while (count < config.getUpdateMaxRecord()) {
//                ArrayList<ReportModel> batch = createBatchFake(config.getUpdateBatchSize(), minId);
//                minId += batch.size();
//                count += batch.size();
//                changePercent(task, count, config.getUpdateMaxRecord());
//                updateBulkData(batch);
//            }
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    class UpdateRunnable implements Runnable {
//        long minId;
//
//        UpdateRunnable(long minId) {
//            this.minId = minId;
//        }
//
//        public void run() {
//            ArrayList<ReportModel> batch = createBatchFake(config.getUpdateBatchSize(), minId);
//            try {
//                updateBulkData(batch);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private void concurrentUpdate() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Update);
//        try {
//            List<Thread> workers = new ArrayList<>();
//            long minId = getMinId();
//            long count = 0;
//            while (count < config.getUpdateMaxRecord()) {
//                Thread worker = new Thread(new UpdateRunnable(minId));
//                workers.add(worker);
//                worker.start();
//                minId += config.getUpdateBatchSize();
//                count += config.getUpdateBatchSize();
//                changePercent(task, count, config.getUpdateMaxRecord());
//            }
//            for (Thread worker : workers) worker.join();
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    private void delete() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Delete);
//        try {
//            long minId = getMinId();
//            DeleteBuilder<ReportModel, Long> deleteBuilder = tableDao.deleteBuilder();
//            long count = 0;
//            while (count < config.getDeleteMaxRecord()) {
//                long maxId = minId + config.getDeleteBatchSize();
//                Where<ReportModel, Long> where = deleteBuilder.where();
//                where.between("id", minId, maxId - 1);
//                minId = maxId;
//                PreparedDelete<ReportModel> preparedQuery = deleteBuilder.prepare();
//                int result = tableDao.delete(preparedQuery);
//                if (result > 0)
//                    count += result;
//                else
//                    count += config.getDeleteBatchSize();
//                deleteBuilder.reset();
//                changePercent(task, count, config.getDeleteMaxRecord());
//            }
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    private class DeleteRunnable implements Runnable {
//        long min;
//        long max;
//
//        DeleteRunnable(long min, long max) {
//            this.min = min;
//            this.max = max;
//        }
//
//        public void run() {
//            try {
//                DeleteBuilder<ReportModel, Long> deleteBuilder = tableDao.deleteBuilder();
//                Where<ReportModel, Long> where = deleteBuilder.where();
//                where.between("id", min, max - 1);
//                PreparedDelete<ReportModel> preparedQuery = deleteBuilder.prepare();
//                tableDao.delete(preparedQuery);
//                deleteBuilder.reset();
//            } catch (Exception ex) {
//                LOGGER.error(ex.getMessage(), ex);
//            }
//        }
//    }
//
//    private void concurrentDelete() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Delete);
//        try {
//            long minId = getMinId();
//            long count = 0;
//            ArrayList<Thread> workers = new ArrayList<>();
//            while (count < config.getDeleteMaxRecord()) {
//                long maxId = minId + config.getDeleteBatchSize();
//                Thread thread = new Thread(new DeleteRunnable(minId, maxId));
//                workers.add(thread);
//                thread.start();
//                minId = maxId;
//                count += config.getDeleteBatchSize();
//                changePercent(task, count, config.getDeleteMaxRecord());
//            }
//            for (Thread worker : workers) {
//                worker.join();
//            }
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    private void fetch() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Fetch);
//        try {
//            long minId = getMinId();
//            QueryBuilder<ReportModel, Long> queryBuilder = tableDao.queryBuilder();
//            long count = 0;
//            while (count < config.getFetchMaxRecord()) {
//                long maxId = minId + config.getFetchBatchSize();
//                Where<ReportModel, Long> where = queryBuilder.where();
//                where.between("id", minId, maxId - 1);
//                minId = maxId;
//                PreparedQuery<ReportModel> preparedQuery = queryBuilder.prepare();
//                List<ReportModel> result = tableDao.query(preparedQuery);
//                if (result.size() > 0)
//                    count += result.size();
//                else
//                    count += config.getFetchBatchSize();
//                queryBuilder.reset();
//                changePercent(task, count, config.getFetchMaxRecord());
//            }
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    private class FetchRunnable implements Runnable {
//        long min;
//        long max;
//
//        FetchRunnable(long min, long max) {
//            this.min = min;
//            this.max = max;
//        }
//
//        public void run() {
//            try {
//                QueryBuilder<ReportModel, Long> queryBuilder = tableDao.queryBuilder();
//                Where<ReportModel, Long> where = queryBuilder.where();
//                where.between("id", min, max - 1);
//                PreparedQuery<ReportModel> preparedQuery = queryBuilder.prepare();
//                List<ReportModel> result = tableDao.query(preparedQuery);
//                result.size();
//                queryBuilder.reset();
//            } catch (Exception ex) {
//                LOGGER.error(ex.getMessage(), ex);
//            }
//        }
//    }
//
//    private void concurrentFetch() {
//        EvaluatorTask task = addTask(EvaluatorTask.TaskType.Fetch);
//        try {
//            long minId = getMinId();
//            long count = 0;
//            ArrayList<Thread> workers = new ArrayList<>();
//            while (count < config.getFetchMaxRecord()) {
//
//                long maxId = minId + config.getFetchBatchSize();
//                Thread thread = new Thread(new FetchRunnable(minId, maxId));
//                workers.add(thread);
//                thread.start();
//                minId = maxId;
//                count += config.getFetchBatchSize();
//                changePercent(task, count, config.getFetchMaxRecord());
//            }
//            for (Thread worker : workers) worker.join();
//        } catch (Exception ex) {
//            task.setException(ex);
//            LOGGER.error(ex.getMessage(), ex);
//        }
//        task.setEndNanoTime(System.nanoTime());
//    }
//
//    private long getMinId() throws SQLException {
//        QueryBuilder<ReportModel, Long> queryBuilder = tableDao.queryBuilder();
//        queryBuilder.selectRaw("MIN(id)");
//        GenericRawResults<String[]> res = tableDao.queryRaw(queryBuilder.prepareStatementString());
//        String[] fr = res.getFirstResult();
//        if (fr == null || fr.length == 0 || fr[0] == null) {
//            ArrayList<ReportModel> list = new ArrayList<>();
//            list.add(createFake());
//            try {
//                insertBulkData(list);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            queryBuilder.reset();
//            queryBuilder.selectRaw("MIN(id)");
//            res = tableDao.queryRaw(queryBuilder.prepareStatementString());
//            fr = res.getFirstResult();
//        }
//        long minId = Long.parseLong(fr[0]);
//        queryBuilder.reset();
//        return minId;
//    }
//
    private void changePercent(EvaluatorTask task, long number, long max) {
        double temp = Math.abs((double) number / max * 100);
        task.setPercent(temp);
    }

    private EvaluatorTask addTask(EvaluatorTask.TaskType type) {
        EvaluatorTask task = new EvaluatorTask(config.getResourceCheckTime().millis());
        task.setType(type);
        result.getTasks().add(task);
        task.checkResource();
        return task;
    }

    private void insertBulkData(List<ReportModel> entities) throws Exception {
        LOGGER.debug("insert " + entities.size() + " in ");
        repository.insertBulk(entities);


        //repository.saveAll(dtos);
//        tableDao.callBatchTasks((Callable<Void>) () -> {
//            for (ReportModel dto : dtos) {
//                tableDao.createOrUpdate(dto);
//            }
//            return null;
//        });

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
//    private void preClean() throws SQLException {
//        if (!config.isClearDatabase())
//            return;
//        TableUtils.clearTable(source, ReportModel.class);
//    }

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
        res.setCount(random.nextInt());
        res.setCurrentCell(random.nextInt());
        res.setCurrentLAC(random.nextInt());
        res.setCurrentVLR(random.nextLong());
        byte[] bytes = new byte[1];
        random.nextBytes(bytes);
        res.setEventType(bytes[0]);
        return res;
    }
}
