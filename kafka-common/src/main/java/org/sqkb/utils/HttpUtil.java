package org.sqkb.utils;


import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpUtil {
    private static Logger log = LoggerFactory.getLogger(HttpUtil.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(60 * 1000, TimeUnit.MILLISECONDS)
            .readTimeout(60 * 1000, TimeUnit.MILLISECONDS)
            .writeTimeout(60 * 1000, TimeUnit.MILLISECONDS)
            .build();

    public static String post(String url, Object req) {
        String json = req == null ? "{}" : com.alibaba.fastjson.JSON.toJSONString(req);
        try {
            RequestBody body = RequestBody.create(JSON, json);
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();
            Response response = client.newCall(request).execute();
            assert response.body() != null;
            return response.body().string();
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
            return "";
        }
    }

    public static String post(String url, String json, Map<String, Object> headers) {
        try {
            RequestBody body = RequestBody.create(JSON, json);
            Request.Builder builder = new Request.Builder()
                    .url(url)
                    .post(body);
            //请求头拼接
            if (headers != null && headers.size() > 0) {
                for (String key : headers.keySet()) {
                    builder.addHeader(key, headers.get(key).toString());
                }
            }
            Request request = builder.build();
            Response response = client.newCall(request).execute();
            return response.body().string();
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
            return "";
        }
    }

    public static String postForm(String url, FormBody formBody) {
        try {
            Request request = new Request.Builder()
                    .url(url)
                    .post(formBody)
                    .build();
            Response response = client.newCall(request).execute();
            return response.body().string();
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
            return "";
        }
    }

    public static String get(String url) {
        try {
            Request request = new Request.Builder()
                    .url(url)
                    .build();
            Response response = client.newCall(request).execute();
            return response.body().string();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }


    /**
     * 请求执行方法，其中包含异常日志及请求时间日志
     *
     * @param request
     * @return
     * @throws Exception
     */
    public static String execute(Request request) throws Exception {
        String res = null;
        try {
            OkHttpClient client = new OkHttpClient();
            Response response = client.newCall(request).execute();
            res = response.body().string();
        } catch (IOException e) {
            throw e;
        }
        return res;
    }

}
