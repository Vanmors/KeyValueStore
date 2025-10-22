package dev.kvstore.controller.request;

import java.util.List;

public record MultiGetRequest(List<String> keys) {
}
