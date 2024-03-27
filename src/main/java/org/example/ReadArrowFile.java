package org.example;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ReadArrowFile {

    public static void main(String[] args) {
        try(
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                FileInputStream fileInputStream = new FileInputStream(new File("random_access_file.arrow"));
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator, new CommonsCompressionFactory());
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                System.out.println("VectorSchemaRoot read: \n" + root.getRowCount());

                Schema schema = root.getSchema();
                List<Field> fields = schema.getFields();
                fields.forEach(field ->
                        System.out.println(field.getName() + ": " + root.getVector(field))
                );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
