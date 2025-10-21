package dev.kvstore.controller.request;

import java.util.List;

public record MultiPutRequest(List<Item> items) {
    public record Item(String key, String value) {
    }
}
