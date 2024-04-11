package org.example.flight;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.example.RandomNumericGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockDataFactory {

    static int no_of_cusips = 10;

//    static RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    static int no_of_yrs = 10;

    static int no_of_observations = no_of_yrs * 365;

    public static List<String> getVectorNames(Map<String, Object> requestMap) {
        String[] cusips = new String[] {"ABC", "XYZ"};

        List<String> facts = (List<String>)requestMap.get("Facts");

        List<String> fields = new ArrayList<>();

        for(String fact: facts) {
            for(String cusip: cusips) {
                fields.add(fact+"."+cusip);
            }
        }

        return fields;

    }

    public static Float4Vector getObservations(String vectorFieldName, RootAllocator allocator) {
        String[] fields = vectorFieldName.split("\\.");
        return getObservations(fields[1], fields[0], allocator);
    }

    public static Float4Vector getObservations(String cusip, String observationName, RootAllocator allocator) {

        Float4Vector fv = new Float4Vector(cusip+"."+observationName, allocator);
        fv.allocateNew(no_of_observations);

        for(int j=0; j < no_of_observations; j++) {
            fv.set(j, RandomNumericGenerator.getRandomFloat());
        }
        fv.setValueCount(no_of_observations);
        return fv;
    }

}
