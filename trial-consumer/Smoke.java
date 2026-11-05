import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import java.time.Duration;
import reactor.core.publisher.Mono;

public final class Smoke {
    public static void main(String[] args) {
        String origin = MySqlConnectionFactory.class.getProtectionDomain().getCodeSource().getLocation().toString();
        if (!origin.contains("consumer-repository")) throw new AssertionError(origin);
        MySqlConnectionFactory factory = MySqlConnectionFactory.from(MySqlConnectionConfiguration.builder()
            .host("127.0.0.1").user("root").password("trial-only")
            .lockWaitTimeout(Duration.ofSeconds(10)).build());
        Long timeout = Mono.usingWhen(factory.create(), c -> c.createStatement(
            "SELECT @@SESSION.innodb_lock_wait_timeout").execute()
            .flatMap(r -> r.map(row -> row.get(0, Long.class))).single(), c -> c.close())
            .block(Duration.ofSeconds(30));
        if (!Long.valueOf(10).equals(timeout)) throw new AssertionError("Timeout: " + timeout);
        System.out.println("Downloaded package connection and configured timeout: PASS; source=" + origin);
    }
}
