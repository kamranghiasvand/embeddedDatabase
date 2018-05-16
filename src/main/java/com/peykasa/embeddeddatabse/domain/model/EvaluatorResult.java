package com.peykasa.embeddeddatabse.domain.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kamran
 */
@Data
public class EvaluatorResult {
    private EvState state;
    private List<EvaluatorTask> tasks=new ArrayList<>();
}
