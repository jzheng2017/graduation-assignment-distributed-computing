package datastorage.dto;

import java.util.Map;

public record DeleteResponse(Map<String, String> prevKeyValues) {
}
