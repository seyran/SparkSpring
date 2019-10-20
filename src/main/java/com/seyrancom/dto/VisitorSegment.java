package com.seyrancom.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class VisitorSegment {
    private String id;
    private String segmentId;

    public static Visitor reduce(VisitorSegment vs1, VisitorSegment vs2) {
        return new Visitor(vs1.getId(), Arrays.asList(vs1.getSegmentId(), vs2.getSegmentId()));
    }
}
