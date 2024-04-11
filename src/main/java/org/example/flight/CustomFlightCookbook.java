package org.example.flight;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.*;

public class CustomFlightCookbook extends NoOpFlightProducer implements AutoCloseable {
    private final BufferAllocator allocator;

    public CustomFlightCookbook(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {

        FlightDescriptor descriptor = reader.getDescriptor();

        Map<String, Object> requestMap = parseRequest(descriptor);
        writeBackResultVectors(requestMap, writer);
    }

    private Map<String, Object> parseRequest(FlightDescriptor descriptor) {
        List<String> pathSteps = descriptor.getPath();

        String request = pathSteps.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<>() {
        };

        HashMap<String,Object> o;

        try {
             o = objectMapper.readValue(request, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return o;
    }

    private void writeBackResultVectors(Map<String, Object> requestMap, ServerStreamListener listener) {

        List<String> fieldNames = MockDataFactory.getVectorNames(requestMap);

        List<Field> fields = new ArrayList<>();

        List<FieldVector> vectors = new ArrayList<>();

        for(String fieldName: fieldNames) {
            Field f = new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
            fields.add(f);

            vectors.add(MockDataFactory.getObservations(fieldName, (RootAllocator) allocator));
        }

        VectorSchemaRoot root1 = new VectorSchemaRoot(fields, vectors);
        VectorUnloader unloader = new VectorUnloader(root1);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(root1.getSchema(),allocator)) {
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);
            loader.load(recordBatch);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public void close() throws Exception {

    }
}
