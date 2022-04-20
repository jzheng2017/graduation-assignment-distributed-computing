package datastorage;

import datastorage.dto.WatchResponse;

public interface WatchListener {
    void onNext(WatchResponse watchResponse);

    void onError(Throwable throwable);

    void onCompleted();
}
