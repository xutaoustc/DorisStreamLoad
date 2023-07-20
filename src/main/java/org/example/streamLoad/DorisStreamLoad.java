package org.example.streamLoad;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.streamLoad.exception.DorisRuntimeException;
import org.example.streamLoad.exception.StreamLoadException;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DorisStreamLoad {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final RecordStream recordStream = new RecordStream(1024 * 1024, 3);
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    CloseableHttpClient httpClient = new HttpUtil().getHttpClient();
    private ExecutorService executorService = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ExecutorThreadFactory("stream-load-upload"));
    private volatile Future<CloseableHttpResponse> pendingLoadFuture;
    private boolean loadBatchFirstRecord;

    // TODO
    private String loadUrlStr = StringUtils.EMPTY;
    private String user;
    private String passwd;
    private Properties streamLoadProp = new Properties();

    public DorisStreamLoad(String address, String db, String table, String user, String passwd) {
        loadUrlStr = String.format(LOAD_URL_PATTERN, address, db, table);
        this.user = user;
        this.passwd = passwd;

        streamLoadProp.setProperty("column_separator", "|");
        streamLoadProp.setProperty("line_delimiter", "\n");
        streamLoadProp.setProperty("format", "json");
        streamLoadProp.setProperty("strict_mode", "true");
        loadBatchFirstRecord = true;
    }

    public void startLoad(String label) throws IOException {
        loadBatchFirstRecord = true;
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        recordStream.startInput();
        InputStreamEntity entity = new InputStreamEntity(recordStream);
        putBuilder.setUrl(loadUrlStr)
                .baseAuth(user, passwd)
                .addCommonHeader()
                .addHiddenColumns(false)
                .setLabel(label)
                .setEntity(entity)
                .addProperties(streamLoadProp);

        pendingLoadFuture = executorService.submit(() -> {
            return httpClient.execute(putBuilder.build());
        });
    }

    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write("\n".getBytes());
        }
        recordStream.write(record);
    }

    public RespContent stopLoad(String label) throws IOException{
        recordStream.endInput();
        System.out.println("stream load stopped for " + label + " on host " + loadUrlStr);
        Preconditions.checkState(pendingLoadFuture != null);
        try {
            return handlePreCommitResponse(pendingLoadFuture.get());
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception{
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 && response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            return OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        }
        throw new StreamLoadException("stream load error: " + response.getStatusLine().toString());
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new IOException("Closing httpClient failed.", e);
            }
        }
        if (null != executorService) {
            executorService.shutdownNow();
        }
    }
}
