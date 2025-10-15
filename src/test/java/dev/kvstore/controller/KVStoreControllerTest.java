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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

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
}
