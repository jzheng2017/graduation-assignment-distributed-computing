package datastorage.dto;

public record WatchEvent(String currentKey, String currentValue, String prevKey, String prevValue, EventType eventType){

    public enum EventType {
        PUT,
        DELETE,
        UNKNOWN
    }
}
