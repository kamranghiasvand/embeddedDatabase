package com.peykasa.embeddeddatabse.domain.model;

import lombok.Data;

/**
 * @author kamran
 */
@Data
public class ReportModel {

    private long id;
    private Long currentVLR;
    private Integer currentLAC;
    private Integer currentCell;
    private String eventType;
    private Integer counter;

    public ReportModel() {
    }

    public ReportModel(long id,Integer counter, long currentVLR, Integer currentLAC, Integer currentCell, String eventType) {
        this.id = id;
        this.currentVLR = currentVLR;
        this.currentLAC = currentLAC;
        this.currentCell = currentCell;
        this.eventType = eventType;
        this.counter=counter;
    }
}