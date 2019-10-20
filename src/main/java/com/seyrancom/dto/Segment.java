package com.seyrancom.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Segment {

    private String id;
    private List<String> visitorIds;

    public Segment() {
    }

    /**
     * First step of deserialization, so intern every string
     *
     * @param id
     * @param visitorIds
     */
    public Segment(String id, List<String> visitorIds) {
        this.id = id.intern();
        this.visitorIds = visitorIds.stream().map(String::intern).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<VisitorSegment> getVisitorIdsMappedList() {
        return getVisitorIds()
                .stream()
                .map(visitorId -> new VisitorSegment(visitorId, getId()))
                .collect(Collectors.toList());
    }

    public static Iterator<VisitorSegment> getVisitorIdsMappedListIterator(Segment segment) {
        return segment.getVisitorIdsMappedList().iterator();
    }
}
