package com.seyrancom.dto;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class RecordBody implements Serializable {

    private String header;
    private List<String> footers = Lists.newArrayList();

}