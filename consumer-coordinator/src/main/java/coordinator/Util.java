package coordinator;

import org.springframework.stereotype.Service;

@Service
public class Util {

    public String getSubstringAfterPrefix(String prefix, String original) {
        int cutOff = original.indexOf(prefix);
        return original.substring(cutOff);
    }
}
