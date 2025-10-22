package dev.kvstore.controller.request;

import java.util.List;

public record MultiDeleteRequest(List<String> keys) {
}
