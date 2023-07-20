package org.example;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.example.streamLoad.DorisStreamLoad;
import org.example.streamLoad.serializer.DorisRecordSerializer;
import org.example.streamLoad.serializer.RowDataSerializer;

import java.io.IOException;

public class DorisWriter {
    private static DorisStreamLoad dorisStreamLoad;

    public static void main(String[] args) throws IOException {
        String label = args[0];
        String address = args[1];
        String db = args[2];
        String table = args[3];
        String user = args[4];
        String passwd = args[5];
        String count = args[6];

        dorisStreamLoad = new DorisStreamLoad(address, db, table, user, passwd);
        dorisStreamLoad.startLoad(label);

        for(int i=0;i <= Integer.parseInt(count);i++) {
            GenericRowData rowData = new GenericRowData(3);
            rowData.setField(0, 1);
            rowData.setField(1, 2);
            rowData.setField(2, 3);
            rowData.setRowKind(RowKind.INSERT);
            byte[] serialize = serializer.serialize(rowData);
            dorisStreamLoad.writeRecord(serialize);
        }

        System.out.println(dorisStreamLoad.stopLoad(label));
        dorisStreamLoad.close();
    }


    private static RowDataSerializer.Builder serializerBuilder = RowDataSerializer.builder();
    static {
        serializerBuilder.setFieldNames(new String[]{"f1","f2","f3"})
                .setFieldType(new LogicalType[]{new IntType(), new IntType(),new IntType()})
                .setType("json")
                .enableDelete(false)
                .setFieldDelimiter("\t");
    }

    private static DorisRecordSerializer<RowData> serializer = serializerBuilder.build();
}
