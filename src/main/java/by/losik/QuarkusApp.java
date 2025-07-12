package by.losik;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@QuarkusMain
@Tag(name = "Launcher", description = "Used for running the app")
public class QuarkusApp implements QuarkusApplication {
    @Override
    @Operation(summary = "Injecting the variables",
            description = "By passing the environment variables, the application registers them, and then the app is run")
    public int run(String... args) {
        Quarkus.run(args);
        return 0;
    }

    @Operation(summary = "Make the app run",
            description = "Launch for application startup")
    public static void main(String... args) {
        Quarkus.run(QuarkusApp.class, args);
    }
}
