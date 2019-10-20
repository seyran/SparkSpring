package com.seyrancom.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class Record implements Serializable {
    private String id;
    private RecordBody body;
}
