package com.peykasa.embeddeddatabse.domain.model;

import lombok.Data;

import javax.persistence.*;

/**
 * @author kamran
 */
@Data
@Entity
@Table(name = "records")
public class ReportModel implements Comparable<ReportModel> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id")
//    @DatabaseField(index = true, generatedId = true)
    private long id;

    @Column(name = "index_u")
    private Integer index;

    @Column(name = "recordTimeStamp")
    private Integer recordTimeStamp;

    @Column(name = "imsi")
    private Long imsi;

    @Column(name = "msisdn")
    private Long msisdn;

    @Column(name = "previousVLR")
    private Long previousVLR;

    @Column(name = "previousLAC")
    private Integer previousLAC;

    @Column(name = "previousCell")
    private Integer previousCell;

    @Column(name = "currentVLR")
    private Long currentVLR;

    @Column(name = "currentLAC")
    private Integer currentLAC;

    @Column(name = "currentCell")
    private Integer currentCell;

    @Column(name = "eventType")
    private Byte eventType;

    @Column(name = "count")
    private Integer count;

    @Column(name = "latitude")
    private Double latitude;

    @Column(name = "longitude")
    private Double longitude;

    @Column(name = "preLatitude")
    private Double preLatitude;

    @Column(name = "preLongitude")
    private Double preLongitude;

    @Override
    public int compareTo(ReportModel o) {
        return imsi.compareTo(o.getImsi());
    }
}