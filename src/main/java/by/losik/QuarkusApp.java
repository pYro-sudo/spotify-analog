package by.losik;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class QuarkusApp implements QuarkusApplication {
    @Override
    public int run(String... args) {
        Quarkus.run(args);
        return 0;
    }

    public static void main(String... args) {
        Quarkus.run(QuarkusApp.class, args);
    }
}
