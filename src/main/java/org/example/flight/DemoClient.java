package org.example.flight;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DemoClient {

    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);

        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();

        Schema schema = new Schema(Arrays.asList(
                new Field("Request", FieldType.nullable(new ArrowType.Utf8()), null)));

        String request = createRequest();
        FlightClient.ExchangeReaderWriter exchangeReaderWriter = flightClient.doExchange(FlightDescriptor.path(request), new CallOption() {});
        FlightClient.ClientStreamListener listener = exchangeReaderWriter.getWriter();
        listener.start(VectorSchemaRoot.create(schema, allocator));
        listener.putNext(); // this steps calls server doExchange by passing it FlightDescriptor.

        FlightStream flightStream = exchangeReaderWriter.getReader();
        VectorSchemaRoot vectorSchemaRoot = flightStream.getRoot();
        while (flightStream.next()) {
            System.out.println(vectorSchemaRoot.contentToTSVString());
        }
    }

    private static String createRequest() {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("Dataset", "Positions");
        requestMap.put("Funds", "123");
        requestMap.put("Facts", new String[] {"DailyGL", "TotalReturn"});

        String request = "";

        try {
            request = objectMapper.writeValueAsString(requestMap);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return request;
    }
}
