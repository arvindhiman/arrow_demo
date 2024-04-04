package org.example.flight;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.example.RandomCusipGenerator;
import org.example.RandomNumericGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME;

public class MockProducer {

    static int no_of_cusips = 10;

//    static RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    static int no_of_yrs = 10;

    static int no_of_observations = no_of_yrs * 365;

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
