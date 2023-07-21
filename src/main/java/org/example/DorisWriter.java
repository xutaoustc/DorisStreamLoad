package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.example.streamLoad.DorisStreamLoad;
import org.example.streamLoad.serializer.DorisRecordSerializer;
import org.example.streamLoad.serializer.RowDataSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DorisWriter {
    private static DorisStreamLoad dorisStreamLoad;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
    private static final int PORT = 9030;   // query_port of Leader Node

    public static void main(String[] args) throws Exception {
        String label = args[0];
        String address = args[1];
        String jdbcAddress = args[2];
        String db = args[3];
        String table = args[4];
        String user = args[5];
        String passwd = args[6];
        String count = args[7];

        List<byte[]> bytes = fetchData(jdbcAddress, db, table, user, passwd, count).stream()
                .map(data -> {
                    try {
                        return objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
                    }catch (Exception e) {
                        return null;
                    }
                }).collect(Collectors.toList());

        System.out.println("begin load");
        long start = System.currentTimeMillis();
        dorisStreamLoad = new DorisStreamLoad(address, db, table, user, passwd);
        dorisStreamLoad.startLoad(label);
        writeData(bytes);
        System.out.println(dorisStreamLoad.stopLoad(label));
        dorisStreamLoad.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    private static void writeData(List<byte[]> bytesList) throws IOException {
        for(byte[] bytes : bytesList) {
            dorisStreamLoad.writeRecord(bytes);
        }
    }

    private static void writeData(String field, String type, List<HttpCollect> datas) throws IOException {
        RowDataSerializer.Builder serializerBuilder = RowDataSerializer.builder();

        LogicalType[] dataTypes = Arrays.stream(type.split(",")).map(t -> {
            LogicalType ret = null;

            switch(t) {
                case "int":
                    ret = new IntType();
                    break;
                case "long":
                    ret = new BigIntType();
                    break;
                case "string":
                    ret = new VarCharType();
                    break;
            }
            return ret;
        }).toArray(LogicalType[] ::new);

        DorisRecordSerializer<RowData> serializer;
        serializerBuilder.setFieldNames(field.split(","))
                .setFieldType(dataTypes)
                .setType("json")
                .enableDelete(false)
                .setFieldDelimiter("\t");
        serializer = serializerBuilder.build();

        for(HttpCollect data : datas) {
            GenericRowData rowData = new GenericRowData(54);
            rowData.setField(0, data.getLog_id());
            rowData.setField(1, data.getRecv_time());
            rowData.setField(2, data.getStart_time());
            rowData.setField(3, data.getEnd_time());
            rowData.setField(4, data.getProtocol());
            rowData.setField(5, data.getAddr_type());
            rowData.setField(6, data.getStream_dir());
            rowData.setField(7, data.getRegion_direction());
            rowData.setField(8, data.getStream_trace_id());
            rowData.setField(9, data.getDevice_id());
            rowData.setField(10, data.getDevice_ip());
            rowData.setField(11, data.getClient_ip());
            rowData.setField(12, data.getInternal_ip());
            rowData.setField(13, data.getClient_port());
            rowData.setField(14, data.getClient_locate());
            rowData.setField(15, data.getClient_asn());
            rowData.setField(16, data.getServer_ip());
            rowData.setField(17, data.getExternal_ip());
            rowData.setField(18, data.getServer_port());
            rowData.setField(19, data.getServer_locate());
            rowData.setField(20, data.getServer_asn());
            rowData.setField(21, data.getL7_protocol());
            rowData.setField(22, data.getProtocol_label());
            rowData.setField(23, data.getC2s_pkt_num());
            rowData.setField(24, data.getS2c_pkt_num());
            rowData.setField(25, data.getC2s_byte_num());
            rowData.setField(26, data.getS2c_byte_num());
            rowData.setField(27, data.getC2s_tcp_lostlen());
            rowData.setField(28, data.getS2c_tcp_lostlen());
            rowData.setField(29, data.getFirst_ttl());
            rowData.setField(30, data.getNest_addr_list());
            rowData.setField(31, data.getTcp_sequence());
            rowData.setField(32, data.getSingle_key());
            rowData.setField(33, data.getHttp_url());
            rowData.setField(34, data.getHttp_referer());
            rowData.setField(35, data.getHttp_host());
            rowData.setField(36, data.getHttp_method());
            rowData.setField(37, data.getHttp_return_code());
            rowData.setField(38, data.getHttp_user_agent());
            rowData.setField(39, data.getHttp_cookie());
            rowData.setField(40, data.getHttp_sequence());
            rowData.setField(41, data.getHttp_version());
            rowData.setField(42, data.getHttp_proxy_flag());
            rowData.setField(43, data.getHttp_set_cookie());
            rowData.setField(44, data.getHttp_request_line());
            rowData.setField(45, data.getHttp_response_line());
            rowData.setField(46, data.getHttp_request_content_type());
            rowData.setField(47, data.getHttp_response_content_type());
            rowData.setField(48, data.getHttp_request_content_len());
            rowData.setField(49, data.getHttp_response_content_len());
            rowData.setField(50, data.getHttp_request_head());
            rowData.setField(51, data.getHttp_response_head());
            rowData.setField(52, data.getHttp_request_body_path());
            rowData.setField(53, data.getHttp_response_body_path());
            rowData.setRowKind(RowKind.INSERT);
            byte[] serialize = serializer.serialize(rowData);
            dorisStreamLoad.writeRecord(serialize);
        }
    }


    public static List<HttpCollect> fetchData(String jdbcAddress, String db, String table, String user, String password, String count) throws Exception{
        Connection conn = null;
        PreparedStatement psUpdate = null;
        String dbUrl = String.format(DB_URL_PATTERN, jdbcAddress, PORT, db);

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(dbUrl, user, password);

        //Query
        Statement stmtQuery = conn.createStatement();
        ResultSet resultSet = stmtQuery.executeQuery( String.format("select * from %s limit %s", table, count) );

        List<HttpCollect> https = new ArrayList<>();
        while (resultSet.next()) {
            HttpCollect http = new HttpCollect();
            http.setLog_id(resultSet.getLong("c_log_id"));
            http.setRecv_time(resultSet.getString("c_recv_time"));
            http.setStart_time(resultSet.getLong("c_start_time"));
            http.setEnd_time(resultSet.getLong("c_end_time"));
            http.setProtocol(resultSet.getString("c_protocol"));
            http.setAddr_type(resultSet.getInt("c_addr_type"));
            http.setStream_dir(resultSet.getInt("c_stream_dir"));
            http.setRegion_direction(resultSet.getInt("c_region_direction"));
            http.setStream_trace_id(resultSet.getLong("c_stream_trace_id"));
            http.setDevice_id(resultSet.getString("c_device_id"));
            http.setDevice_ip(resultSet.getString("c_device_ip"));
            http.setClient_ip(resultSet.getString("c_client_ip"));
            http.setInternal_ip(resultSet.getString("c_internal_ip"));
            http.setClient_port(resultSet.getInt("c_client_port"));
            http.setClient_locate(resultSet.getString("c_client_locate"));
            http.setClient_asn(resultSet.getString("c_client_asn"));
            http.setServer_ip(resultSet.getString("c_server_ip"));
            http.setExternal_ip(resultSet.getString("c_external_ip"));
            http.setServer_port(resultSet.getInt("c_server_port"));
            http.setServer_locate(resultSet.getString("c_server_locate"));
            http.setServer_asn(resultSet.getString("c_server_asn"));
            http.setL7_protocol(resultSet.getString("c_l7_protocol"));
            http.setProtocol_label(resultSet.getString("c_protocol_label"));
            http.setC2s_pkt_num(resultSet.getLong("c_c2s_pkt_num"));
            http.setS2c_pkt_num(resultSet.getLong("c_s2c_pkt_num"));
            http.setC2s_byte_num(resultSet.getLong("c_c2s_byte_num"));
            http.setS2c_byte_num(resultSet.getLong("c_s2c_byte_num"));
            http.setC2s_tcp_lostlen(resultSet.getLong("c_c2s_tcp_lostlen"));
            http.setS2c_tcp_lostlen(resultSet.getLong("c_s2c_tcp_lostlen"));
            http.setFirst_ttl(resultSet.getInt("c_first_ttl"));
            http.setNest_addr_list(resultSet.getString("c_nest_addr_list"));
            http.setTcp_sequence(resultSet.getLong("c_tcp_sequence"));
            http.setSingle_key(resultSet.getString("c_single_key"));
            http.setHttp_url(resultSet.getString("c_http_url"));
            http.setHttp_referer(resultSet.getString("c_http_referer"));
            http.setHttp_host(resultSet.getString("c_http_host"));
            http.setHttp_method(resultSet.getString("c_http_method"));
            http.setHttp_return_code(resultSet.getString("c_http_return_code"));
            http.setHttp_user_agent(resultSet.getString("c_http_user_agent"));
            http.setHttp_cookie(resultSet.getString("c_http_cookie"));
            http.setHttp_sequence(resultSet.getInt("c_http_sequence") + 1);
            http.setHttp_version(resultSet.getString("c_http_version"));
            http.setHttp_proxy_flag(resultSet.getInt("c_http_proxy_flag"));
            http.setHttp_set_cookie(resultSet.getString("c_http_set_cookie"));
            http.setHttp_request_line(resultSet.getString("c_http_request_line"));
            http.setHttp_response_line(resultSet.getString("c_http_response_line"));
            http.setHttp_request_content_type(resultSet.getString("c_http_request_content_type"));
            http.setHttp_response_content_type(resultSet.getString("c_http_response_content_type"));
            http.setHttp_request_content_len(resultSet.getString("c_http_request_content_len"));
            http.setHttp_response_content_len(resultSet.getString("c_http_response_content_len"));
            http.setHttp_request_head(resultSet.getString("c_http_request_head"));
            http.setHttp_response_head(resultSet.getString("c_http_reponse_head"));
            http.setHttp_request_body_path(resultSet.getString("c_http_request_body_path"));
            http.setHttp_response_body_path(resultSet.getString("c_http_response_body_path"));
            https.add(http);
        }

        return https;
    }
}
