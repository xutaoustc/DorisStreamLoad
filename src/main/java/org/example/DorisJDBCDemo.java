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

import com.fasterxml.jackson.annotation.JsonProperty;
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
                HttpCollect data = https.get(i);
                psUpdate.setLong(1, data.getLog_id());
                psUpdate.setString(2, data.getRecv_time());
                psUpdate.setLong(3, data.getStart_time());
                psUpdate.setLong(4, data.getEnd_time());
                psUpdate.setString(5, data.getProtocol());
                psUpdate.setInt(6, data.getAddr_type());
                psUpdate.setInt(7, data.getStream_dir());
                psUpdate.setInt(8, data.getRegion_direction());
                psUpdate.setLong(9, data.getStream_trace_id());
                psUpdate.setString(10, data.getDevice_id());
                psUpdate.setString(11, data.getDevice_ip());
                psUpdate.setString(12, data.getClient_ip());
                psUpdate.setString(13, data.getInternal_ip());
                psUpdate.setInt(14, data.getClient_port());
                psUpdate.setString(15, data.getClient_locate());
                psUpdate.setString(16, data.getClient_asn());
                psUpdate.setString(17, data.getServer_ip());
                psUpdate.setString(18, data.getExternal_ip());
                psUpdate.setInt(19, data.getServer_port());
                psUpdate.setString(20, data.getServer_locate());
                psUpdate.setString(21, data.getServer_asn());
                psUpdate.setString(22, data.getL7_protocol());
                psUpdate.setString(23, data.getProtocol_label());
                psUpdate.setLong(24, data.getC2s_pkt_num());
                psUpdate.setLong(25, data.getS2c_pkt_num());
                psUpdate.setLong(26, data.getC2s_byte_num());
                psUpdate.setLong(27, data.getS2c_byte_num());
                psUpdate.setLong(28, data.getC2s_tcp_lostlen());
                psUpdate.setLong(29, data.getS2c_tcp_lostlen());
                psUpdate.setInt(30, data.getFirst_ttl());
                psUpdate.setString(31, data.getNest_addr_list());
                psUpdate.setLong(32, data.getTcp_sequence());
                psUpdate.setString(33, data.getSingle_key());
                psUpdate.setString(34, data.getHttp_url());
                psUpdate.setString(35, data.getHttp_referer());
                psUpdate.setString(36, data.getHttp_host());
                psUpdate.setString(37, data.getHttp_method());
                psUpdate.setString(38, data.getHttp_return_code());
                psUpdate.setString(39, data.getHttp_user_agent());
                psUpdate.setString(40, data.getHttp_cookie());
                psUpdate.setInt(41, data.getHttp_sequence());
                psUpdate.setString(42, data.getHttp_version());
                psUpdate.setInt(43, data.getHttp_proxy_flag());
                psUpdate.setString(44, data.getHttp_set_cookie());
                psUpdate.setString(45, data.getHttp_request_line());
                psUpdate.setString(46, data.getHttp_response_line());
                psUpdate.setString(47, data.getHttp_request_content_type());
                psUpdate.setString(48, data.getHttp_response_content_type());
                psUpdate.setString(49, data.getHttp_request_content_len());
                psUpdate.setString(50, data.getHttp_response_content_len());
                psUpdate.setString(51, data.getHttp_request_head());
                psUpdate.setString(52, data.getHttp_response_head());
                psUpdate.setString(53, data.getHttp_request_body_path());
                psUpdate.setString(54, data.getHttp_response_body_path());

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
    @JsonProperty("c_log_id")
    private Long log_id;
    @JsonProperty("c_recv_time")
    private String recv_time;
    @JsonProperty("c_start_time")
    private Long start_time;
    @JsonProperty("c_end_time")
    private Long end_time;
    @JsonProperty("c_protocol")
    private String protocol;
    @JsonProperty("c_addr_type")
    private Integer addr_type;
    @JsonProperty("c_stream_dir")
    private Integer stream_dir;
    @JsonProperty("c_region_direction")
    private Integer region_direction;
    @JsonProperty("c_stream_trace_id")
    private Long stream_trace_id;
    @JsonProperty("c_device_id")
    private String device_id;
    @JsonProperty("c_device_ip")
    private String device_ip;
    @JsonProperty("c_client_ip")
    private String client_ip;
    @JsonProperty("c_internal_ip")
    private String internal_ip;
    @JsonProperty("c_client_port")
    private Integer client_port;
    @JsonProperty("c_client_locate")
    private String client_locate;
    @JsonProperty("c_client_asn")
    private String client_asn;
    @JsonProperty("c_server_ip")
    private String server_ip;
    @JsonProperty("c_external_ip")
    private String external_ip;
    @JsonProperty("c_server_port")
    private Integer server_port;
    @JsonProperty("c_server_locate")
    private String server_locate;
    @JsonProperty("c_server_asn")
    private String server_asn;
    @JsonProperty("c_l7_protocol")
    private String l7_protocol;
    @JsonProperty("c_protocol_label")
    private String protocol_label;
    @JsonProperty("c_c2s_pkt_num")
    private Long c2s_pkt_num;
    @JsonProperty("c_s2c_pkt_num")
    private Long s2c_pkt_num;
    @JsonProperty("c_c2s_byte_num")
    private Long c2s_byte_num;
    @JsonProperty("c_s2c_byte_num")
    private Long s2c_byte_num;
    @JsonProperty("c_c2s_tcp_lostlen")
    private Long c2s_tcp_lostlen;
    @JsonProperty("c_s2c_tcp_lostlen")
    private Long s2c_tcp_lostlen;
    @JsonProperty("c_first_ttl")
    private Integer first_ttl;
    @JsonProperty("c_nest_addr_list")
    private String nest_addr_list;
    @JsonProperty("c_tcp_sequence")
    private Long tcp_sequence;
    @JsonProperty("c_single_key")
    private String single_key;
    @JsonProperty("c_http_url")
    private String http_url;
    @JsonProperty("c_http_referer")
    private String http_referer;
    @JsonProperty("c_http_host")
    private String http_host;
    @JsonProperty("c_http_method")
    private String http_method;
    @JsonProperty("c_http_return_code")
    private String http_return_code;
    @JsonProperty("c_http_user_agent")
    private String http_user_agent;
    @JsonProperty("c_http_cookie")
    private String http_cookie;
    @JsonProperty("c_http_sequence")
    private Integer http_sequence;
    @JsonProperty("c_http_version")
    private String http_version;
    @JsonProperty("c_http_proxy_flag")
    private Integer http_proxy_flag;
    @JsonProperty("c_http_set_cookie")
    private String http_set_cookie;
    @JsonProperty("c_http_request_line")
    private String http_request_line;
    @JsonProperty("c_http_response_line")
    private String http_response_line;
    @JsonProperty("c_http_request_content_type")
    private String http_request_content_type;
    @JsonProperty("c_http_response_content_type")
    private String http_response_content_type;
    @JsonProperty("c_http_request_content_len")
    private String http_request_content_len;
    @JsonProperty("c_http_response_content_len")
    private String http_response_content_len;
    @JsonProperty("c_http_request_head")
    private String http_request_head;
    @JsonProperty("c_http_reponse_head")
    private String http_response_head;
    @JsonProperty("c_http_request_body_path")
    private String http_request_body_path;
    @JsonProperty("c_http_response_body_path")
    private String http_response_body_path;
}