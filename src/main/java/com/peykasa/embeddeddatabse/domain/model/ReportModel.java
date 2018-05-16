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
    private Byte eventType;
    private Integer count;

}