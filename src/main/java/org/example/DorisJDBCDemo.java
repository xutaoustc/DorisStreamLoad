package org.example;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

public class DorisJDBCDemo {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
    private static final int PORT = 9030;   // query_port of Leader Node

    public static void main(String[] args) {
        String as = "default_cluster:test310";
        String ss = "default_cluster";
        System.out.println(as.substring(ss.length()));


        String db = args[0];
        String table = args[1];
        String user = args[2];
        String password = args[3];
        String host = args[4];
        Integer all = Integer.parseInt(args[5]);

        Connection conn = null;
        PreparedStatement psUpdate = null;
        String dbUrl = String.format(DB_URL_PATTERN, host, PORT, db);
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(dbUrl, user, password);
//            stmt = conn.prepareStatement(query);

            //Query
            Statement stmtQuery = conn.createStatement();
            ResultSet resultSet = stmtQuery.executeQuery( String.format("select * from %s limit %s", table, all) );

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




            psUpdate = conn.prepareStatement( "INSERT into t_http_collect_log_1500_20_bloom values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" );

            for (int i =0; i < https.size(); i++) {
                HttpCollect e = https.get(i);
                psUpdate.setLong(1, e.getLog_id());
                psUpdate.setString(2, e.getRecv_time());
                psUpdate.setLong(3, e.getStart_time());
                psUpdate.setLong(4, e.getEnd_time());
                psUpdate.setString(5, e.getProtocol());
                psUpdate.setInt(6, e.getAddr_type());
                psUpdate.setInt(7, e.getStream_dir());
                psUpdate.setInt(8, e.getRegion_direction());
                psUpdate.setLong(9, e.getStream_trace_id());
                psUpdate.setString(10, e.getDevice_id());
                psUpdate.setString(11, e.getDevice_ip());
                psUpdate.setString(12, e.getClient_ip());
                psUpdate.setString(13, e.getInternal_ip());
                psUpdate.setInt(14, e.getClient_port());
                psUpdate.setString(15, e.getClient_locate());
                psUpdate.setString(16, e.getClient_asn());
                psUpdate.setString(17, e.getServer_ip());
                psUpdate.setString(18, e.getExternal_ip());
                psUpdate.setInt(19, e.getServer_port());
                psUpdate.setString(20, e.getServer_locate());
                psUpdate.setString(21, e.getServer_asn());
                psUpdate.setString(22, e.getL7_protocol());
                psUpdate.setString(23, e.getProtocol_label());
                psUpdate.setLong(24, e.getC2s_pkt_num());
                psUpdate.setLong(25, e.getS2c_pkt_num());
                psUpdate.setLong(26, e.getC2s_byte_num());
                psUpdate.setLong(27, e.getS2c_byte_num());
                psUpdate.setLong(28, e.getC2s_tcp_lostlen());
                psUpdate.setLong(29, e.getS2c_tcp_lostlen());
                psUpdate.setInt(30, e.getFirst_ttl());
                psUpdate.setString(31, e.getNest_addr_list());
                psUpdate.setLong(32, e.getTcp_sequence());
                psUpdate.setString(33, e.getSingle_key());
                psUpdate.setString(34, e.getHttp_url());
                psUpdate.setString(35, e.getHttp_referer());
                psUpdate.setString(36, e.getHttp_host());
                psUpdate.setString(37, e.getHttp_method());
                psUpdate.setString(38, e.getHttp_return_code());
                psUpdate.setString(39, e.getHttp_user_agent());
                psUpdate.setString(40, e.getHttp_cookie());
                psUpdate.setInt(41, e.getHttp_sequence());
                psUpdate.setString(42, e.getHttp_version());
                psUpdate.setInt(43, e.getHttp_proxy_flag());
                psUpdate.setString(44, e.getHttp_set_cookie());
                psUpdate.setString(45, e.getHttp_request_line());
                psUpdate.setString(46, e.getHttp_response_line());
                psUpdate.setString(47, e.getHttp_request_content_type());
                psUpdate.setString(48, e.getHttp_response_content_type());
                psUpdate.setString(49, e.getHttp_request_content_len());
                psUpdate.setString(50, e.getHttp_response_content_len());
                psUpdate.setString(51, e.getHttp_request_head());
                psUpdate.setString(52, e.getHttp_response_head());
                psUpdate.setString(53, e.getHttp_request_body_path());
                psUpdate.setString(54, e.getHttp_response_body_path());

                psUpdate.addBatch();
            }

            long start = System.currentTimeMillis();
            int[] res = psUpdate.executeBatch();
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (psUpdate != null) {
                    psUpdate.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}

@Data
class HttpCollect {
    private Long log_id;
    private String recv_time;
    private Long start_time;
    private Long end_time;
    private String protocol;
    private Integer addr_type;
    private Integer stream_dir;
    private Integer region_direction;
    private Long stream_trace_id;
    private String device_id;
    private String device_ip;
    private String client_ip;
    private String internal_ip;
    private Integer client_port;
    private String client_locate;
    private String client_asn;
    private String server_ip;
    private String external_ip;
    private Integer server_port;
    private String server_locate;
    private String server_asn;
    private String l7_protocol;
    private String protocol_label;
    private Long c2s_pkt_num;
    private Long s2c_pkt_num;
    private Long c2s_byte_num;
    private Long s2c_byte_num;
    private Long c2s_tcp_lostlen;
    private Long s2c_tcp_lostlen;
    private Integer first_ttl;
    private String nest_addr_list;
    private Long tcp_sequence;
    private String single_key;
    private String http_url;
    private String http_referer;
    private String http_host;
    private String http_method;
    private String http_return_code;
    private String http_user_agent;
    private String http_cookie;
    private Integer http_sequence;
    private String http_version;
    private Integer http_proxy_flag;
    private String http_set_cookie;
    private String http_request_line;
    private String http_response_line;
    private String http_request_content_type;
    private String http_response_content_type;
    private String http_request_content_len;
    private String http_response_content_len;
    private String http_request_head;
    private String http_response_head;
    private String http_request_body_path;
    private String http_response_body_path;
}