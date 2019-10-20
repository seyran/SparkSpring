package com.seyrancom.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Visitor {

    private String id;
    private List<String> segmentIds;


/*    public static Visitor reduce(Visitor visitor1, Visitor visitor2) {
        return new Visitor(visitor1.getId(), );
    }*/
}
