package dev.kvstore.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.kvstore.controller.request.DeleteRequest;
import dev.kvstore.controller.request.PutRequest;
import dev.kvstore.core.KVException;
import dev.kvstore.core.KeyValueStore;
import dev.kvstore.core.model.DeleteResult;
import dev.kvstore.core.model.GetResult;
import dev.kvstore.core.model.PutResult;
import dev.kvstore.core.model.ValueRecord;
import dev.kvstore.controller.request.MultiPutRequest;
import dev.kvstore.controller.request.MultiGetRequest;
import dev.kvstore.controller.request.MultiDeleteRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = KVStoreController.class)
class KVStoreControllerTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper om;

    @MockitoBean
    KeyValueStore keyValueStore;

    @Test
    @DisplayName("GET /kvstore/get - 200 OK, found=true, корректный JSON")
    void get_found_ok() throws Exception {
        var valueBytes = "v1".getBytes(StandardCharsets.UTF_8);
        var vr = new ValueRecord(valueBytes, 7L, 0L);
        var gr = new GetResult(true, vr);

        when(keyValueStore.get(eq("k1".getBytes(StandardCharsets.UTF_8)))).thenReturn(gr);

        mvc.perform(get("/kvstore/get").param("key", "k1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.value", is("v1")))
                .andExpect(jsonPath("$.version", is(7)))
                .andExpect(jsonPath("$.expire", is(0)));
    }

    @Test
    @DisplayName("GET /kvstore/get - 404 Not Found, когда found=false")
    void get_not_found_404() throws Exception {
        var gr = new GetResult(false, null);

        when(keyValueStore.get(eq("absent".getBytes(StandardCharsets.UTF_8)))).thenReturn(gr);

        mvc.perform(get("/kvstore/get").param("key", "absent"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error", containsString("value not found")));
    }

    @Test
    @DisplayName("GET /kvstore/get - корректная работа с UTF-8 ключами")
    void get_utf8_key() throws Exception {
        String key = "ключ";
        byte[] expected = key.getBytes(StandardCharsets.UTF_8);

        var vr = new ValueRecord("значение".getBytes(StandardCharsets.UTF_8), 1L, 0L);
        var gr = new GetResult(true, vr);

        when(keyValueStore.get(eq(expected))).thenReturn(gr);

        mvc.perform(get("/kvstore/get").param("key", key))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value", is("значение")))
                .andExpect(jsonPath("$.version", is(1)));
    }

    @Test
    @DisplayName("GET /kvstore/get - 500 при исключении")
    void get_error_500() throws Exception {
        when(keyValueStore.get(any())).thenThrow(new KVException("boom"));

        mvc.perform(get("/kvstore/get").param("key", "k1"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error", is("boom")));
    }

    @Test
    @DisplayName("POST /kvstore/get - 200 OK, found=true, корректный JSON")
    void get_post_found_ok() throws Exception {
        var valueBytes = "v1".getBytes(StandardCharsets.UTF_8);
        var vr = new ValueRecord(valueBytes, 7L, 0L);
        var gr = new GetResult(true, vr);

        when(keyValueStore.get(eq("k1".getBytes(StandardCharsets.UTF_8)))).thenReturn(gr);

        var body = new dev.kvstore.controller.request.GetRequest("k1");
        mvc.perform(post("/kvstore/get")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.value", is("v1")))
                .andExpect(jsonPath("$.version", is(7)))
                .andExpect(jsonPath("$.expire", is(0)));
    }

    @Test
    @DisplayName("POST /kvstore/get - 404 Not Found, когда found=false")
    void get_post_not_found_404() throws Exception {
        when(keyValueStore.get(eq("absent".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new GetResult(false, null));

        var body = new dev.kvstore.controller.request.GetRequest("absent");
        mvc.perform(post("/kvstore/get")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error", containsString("value not found")));
    }

    @Test
    @DisplayName("POST /kvstore/get - 400 когда key отсутствует")
    void get_post_400_when_key_missing() throws Exception {
        mvc.perform(post("/kvstore/get")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":null}".getBytes(StandardCharsets.UTF_8)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("key is required")));
    }

    @Test
    @DisplayName("POST /kvstore/get - 500 при исключении")
    void get_post_error_500() throws Exception {
        when(keyValueStore.get(any())).thenThrow(new KVException("boom"));

        var body = new dev.kvstore.controller.request.GetRequest("k1");
        mvc.perform(post("/kvstore/get")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error", is("boom")));
    }


    @Test
    @DisplayName("POST /kvstore/put - 200 OK, created=true")
    void put_created_true() throws Exception {
        var pr = new PutResult(true);

        when(keyValueStore.put(
                eq("k1".getBytes(StandardCharsets.UTF_8)),
                eq("v1".getBytes(StandardCharsets.UTF_8))
        )).thenReturn(pr);

        var body = new PutRequest("k1", "v1");
        mvc.perform(post("/kvstore/put")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.created", is(true)));
    }

    @Test
    @DisplayName("POST /kvstore/put - 200 OK, created=false (обновление)")
    void put_created_false_update() throws Exception {
        var pr = new PutResult(false);

        when(keyValueStore.put(
                eq("k1".getBytes(StandardCharsets.UTF_8)),
                eq("v2".getBytes(StandardCharsets.UTF_8))
        )).thenReturn(pr);

        var body = new PutRequest("k1", "v2");
        mvc.perform(post("/kvstore/put")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.created", is(false)));
    }

    @Test
    @DisplayName("POST /kvstore/put - 500 при исключении")
    void put_error_500() throws Exception {
        when(keyValueStore.put(any(), any())).thenThrow(new KVException("write-failed"));

        var body = new PutRequest("k", "v");
        mvc.perform(post("/kvstore/put")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error", is("write-failed")));
    }


    @Test
    @DisplayName("POST /kvstore/delete - 200 OK, success=true")
    void delete_success_true() throws Exception {
        var dr = new DeleteResult(true);

        when(keyValueStore.delete(eq("k1".getBytes(StandardCharsets.UTF_8)))).thenReturn(dr);

        var body = new DeleteRequest("k1");
        mvc.perform(post("/kvstore/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)));
    }

    @Test
    @DisplayName("POST /kvstore/delete - 200 OK, success=false")
    void delete_success_false() throws Exception {
        var dr = new DeleteResult(false);

        when(keyValueStore.delete(eq("absent".getBytes(StandardCharsets.UTF_8)))).thenReturn(dr);

        var body = new DeleteRequest("absent");
        mvc.perform(post("/kvstore/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(false)));
    }

    @Test
    @DisplayName("POST /kvstore/delete - 500 при исключении")
    void delete_error_500() throws Exception {
        when(keyValueStore.delete(any())).thenThrow(new KVException("delete-failed"));

        var body = new DeleteRequest("k1");
        mvc.perform(post("/kvstore/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error", is("delete-failed")));
    }

    @Test
    @DisplayName("POST /kvstore/mput - проверка статусов")
    void mput_partial_success() throws Exception {
        // k1 -> created=true, k2 -> throw, k3 -> created=false
        when(keyValueStore.put(eq("k1".getBytes(StandardCharsets.UTF_8)),
                eq("v1".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new PutResult(true));

        when(keyValueStore.put(eq("k2".getBytes(StandardCharsets.UTF_8)),
                eq("v2".getBytes(StandardCharsets.UTF_8))))
                .thenThrow(new KVException("boom-2"));

        when(keyValueStore.put(eq("k3".getBytes(StandardCharsets.UTF_8)),
                eq("v3".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new PutResult(false));

        var body = new MultiPutRequest(List.of(
                new MultiPutRequest.Item("k1", "v1"),
                new MultiPutRequest.Item("k2", "v2"),
                new MultiPutRequest.Item("k3", "v3")
        ));

        mvc.perform(post("/kvstore/mput")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.successCount", is(2)))
                .andExpect(jsonPath("$.failureCount", is(1)))
                .andExpect(jsonPath("$.results", hasSize(3)))
                .andExpect(jsonPath("$.results[0].key", is("k1")))
                .andExpect(jsonPath("$.results[0].success", is(true)))
                .andExpect(jsonPath("$.results[0].created", is(true)))
                .andExpect(jsonPath("$.results[1].key", is("k2")))
                .andExpect(jsonPath("$.results[1].success", is(false)))
                .andExpect(jsonPath("$.results[1].error", containsString("boom-2")))
                .andExpect(jsonPath("$.results[2].key", is("k3")))
                .andExpect(jsonPath("$.results[2].success", is(true)))
                .andExpect(jsonPath("$.results[2].created", is(false)));
    }

    @Test
    @DisplayName("POST /kvstore/mget - смешанные found/absent/ошибка - корректный per-item ответ")
    void mget_mixed() throws Exception {
        var vr1 = new ValueRecord("v1".getBytes(StandardCharsets.UTF_8), 5L, 0L);
        when(keyValueStore.get(eq("k1".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new GetResult(true, vr1));

        // absent
        when(keyValueStore.get(eq("absent".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new GetResult(false, null));

        // ошибка
        when(keyValueStore.get(eq("bad".getBytes(StandardCharsets.UTF_8))))
                .thenThrow(new KVException("get-failed"));

        var body = new MultiGetRequest(List.of("k1", "absent", "bad"));

        mvc.perform(post("/kvstore/mget")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results", hasSize(3)))
                .andExpect(jsonPath("$.results[0].key", is("k1")))
                .andExpect(jsonPath("$.results[0].found", is(true)))
                .andExpect(jsonPath("$.results[0].value", is("v1")))
                .andExpect(jsonPath("$.results[0].version", is(5)))
                .andExpect(jsonPath("$.results[1].key", is("absent")))
                .andExpect(jsonPath("$.results[1].found", is(false)))
                .andExpect(jsonPath("$.results[2].key", is("bad")))
                .andExpect(jsonPath("$.results[2].found", is(false)))
                .andExpect(jsonPath("$.results[2].error", containsString("get-failed")));
    }

    @Test
    @DisplayName("POST /kvstore/mput - 400 когда items отсутствует")
    void mput_400_when_items_missing() throws Exception {
        mvc.perform(post("/kvstore/mput")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"items\":null}".getBytes(StandardCharsets.UTF_8)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("items is required")));
    }

    @Test
    @DisplayName("POST /kvstore/mget - 400 когда keys отсутствует")
    void mget_400_when_keys_missing() throws Exception {
        mvc.perform(post("/kvstore/mget")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"keys\":null}".getBytes(StandardCharsets.UTF_8)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("keys is required")));
    }

    @Test
    @DisplayName("POST /kvstore/mdelete - статусы")
    void mdelete_partial_success() throws Exception {
        // k1 -> true, k2 -> exception, absent -> false
        when(keyValueStore.delete(eq("k1".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new DeleteResult(true));

        when(keyValueStore.delete(eq("k2".getBytes(StandardCharsets.UTF_8))))
                .thenThrow(new KVException("del-failed-2"));

        when(keyValueStore.delete(eq("absent".getBytes(StandardCharsets.UTF_8))))
                .thenReturn(new DeleteResult(false));

        var body = new MultiDeleteRequest(java.util.List.of("k1", "k2", "absent"));

        mvc.perform(post("/kvstore/mdelete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.successCount", is(1)))
                .andExpect(jsonPath("$.failureCount", is(2)))
                .andExpect(jsonPath("$.results", hasSize(3)))
                .andExpect(jsonPath("$.results[0].key", is("k1")))
                .andExpect(jsonPath("$.results[0].success", is(true)))
                .andExpect(jsonPath("$.results[1].key", is("k2")))
                .andExpect(jsonPath("$.results[1].success", is(false)))
                .andExpect(jsonPath("$.results[1].error", containsString("del-failed-2")))
                .andExpect(jsonPath("$.results[2].key", is("absent")))
                .andExpect(jsonPath("$.results[2].success", is(false)));
    }

    @Test
    @DisplayName("POST /kvstore/mdelete - 400 когда keys отсутствует")
    void mdelete_400_when_keys_missing() throws Exception {
        mvc.perform(post("/kvstore/mdelete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"keys\":null}".getBytes(StandardCharsets.UTF_8)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("keys is required")));
    }

}
