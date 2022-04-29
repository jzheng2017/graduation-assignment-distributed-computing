package worker;

import org.springframework.stereotype.Service;

@Service
public class Util {

    public String getSubstringAfterPrefix(String prefix, String original) {
        int cutOff = original.indexOf(prefix) + prefix.length();
        return original.substring(cutOff);
    }
}
