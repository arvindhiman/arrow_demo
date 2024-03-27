package org.example;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Arrays.asList;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME;

import org.apache.arrow.vector.Float4Vector;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    static int no_of_cusips = 100_000;
    static int no_of_yrs = 10;

    static int no_of_observations = no_of_yrs * 365;

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        List fields = new ArrayList<Field>();
        for(int i = 0; i < no_of_cusips; i++) {
            fields.add(new Field("dailyGL" + i,
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    /*children*/null
            ));
        }

        Schema schema = new Schema(fields, /*metadata*/ null);
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.setRowCount(no_of_observations);

        for(int i=0; i < no_of_cusips; i++) {
            String cusip = RandomCusipGenerator.getRandom();
            Float4Vector fv = (Float4Vector) root.getVector("dailyGL" + i);
//            Float4Vector fv = new Float4Vector(cusip, allocator);
            fv.allocateNew(no_of_observations);
            for(int j=0; j < no_of_observations; j++) {
                fv.set(j, RandomNumericGenerator.getRandomFloat());
            }
            fv.setValueCount(no_of_observations);
//            System.out.println(fv);
        }
//        System.out.println(root.getRowCount());
        long endTime = System.currentTimeMillis();
        System.out.println("Total Time: " + (endTime - startTime));

        File file = new File("random_access_file.arrow");
        try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                ArrowFileWriter writer = new ArrowFileWriter(root, /*provider*/ null, fileOutputStream.getChannel()
                , /*metadata*/ new HashMap<>(), /*ipcoptions*/IpcOption.DEFAULT, new CommonsCompressionFactory(), LZ4_FRAME);
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size()
                    + ". Number of rows written: " + root.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}