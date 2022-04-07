package messagequeue.configuration;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class CoreApplicationStartupHandler implements ApplicationRunner {
    private EnvironmentSetup environmentSetup;

    public CoreApplicationStartupHandler(EnvironmentSetup environmentSetup) {
        this.environmentSetup = environmentSetup;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        environmentSetup.setup();
    }
}
