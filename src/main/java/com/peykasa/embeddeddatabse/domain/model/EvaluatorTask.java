package com.peykasa.embeddeddatabse.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author kamran
 */
@Data
public class EvaluatorTask {
    private TaskType type;
    private double percent;
    private List<Long> usedRam = new ArrayList<>();
    private long startNanoTime;
    private long endNanoTime;
    private Exception exception;
    private long totalTime;
    @JsonIgnore
    private Timer timer;
    @JsonIgnore
    ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1);
    public EvaluatorTask(long resourceCheckMilis) {
        setStartNanoTime(System.nanoTime());
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {

                checkResource();
            }
        },0,resourceCheckMilis);
    }

    public void checkResource() {
        usedRam.add(Runtime.getRuntime().totalMemory());
    }

    public long getEndNanoTime() {
        return endNanoTime;
    }

    public void setEndNanoTime(long endNanoTime) {
        checkResource();
        this.endNanoTime = endNanoTime;
        timer.cancel();
    }

    public long getTotalTime() {
        totalTime = endNanoTime - startNanoTime;
        return totalTime / 1000000;
    }

    public enum TaskType {
        Insert,
        Update,
        Delete,
        Fetch
    }
}
