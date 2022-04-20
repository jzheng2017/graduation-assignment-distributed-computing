package datastorage.dto;

import java.util.List;

public record WatchResponse(List<WatchEvent> events) {
}
