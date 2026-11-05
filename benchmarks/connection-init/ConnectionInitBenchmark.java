import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConnectionInitBenchmark {
    static final MySqlConnectionFactory FACTORY = MySqlConnectionFactory.from(
        MySqlConnectionConfiguration.builder().host("127.0.0.1").port(3306)
            .user("bench").password("benchmark-only").database("bench")
            .sslMode(SslMode.DISABLED).connectTimeout(Duration.ofSeconds(10)).build());

    public static void main(String[] args) {
        String variant = args[0];
        int concurrency = Integer.parseInt(args[1]);
        int count = Integer.parseInt(args[2]);
        String mode = args[3];
        String sql = variant.equals("baseline")
            ? "SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'"
            : "SELECT CAST(@@SESSION.innodb_lock_wait_timeout AS CHAR)";
        int column = variant.equals("baseline") ? 1 : 0;
        long[] timings = new long[count];
        AtomicInteger errors = new AtomicInteger();
        if (mode.equals("sql")) {
            // Persistent connections isolate the variable query from handshake and authentication.
            runSql(sql, column, concurrency, 512, new long[512], new AtomicInteger());
        } else {
            runConnections(concurrency, 256, new long[256], new AtomicInteger());
        }
        long start = System.nanoTime();
        if (mode.equals("sql")) runSql(sql, column, concurrency, count, timings, errors);
        else runConnections(concurrency, count, timings, errors);
        double seconds = (System.nanoTime() - start) / 1e9;
        long[] success = Arrays.stream(timings).filter(v -> v > 0).sorted().toArray();
        if (success.length == 0) throw new AssertionError("No successful measurements");
        System.out.printf(Locale.ROOT,
            "RESULT {\"variant\":\"%s\",\"mode\":\"%s\",\"concurrency\":%d,\"count\":%d,\"errors\":%d,\"seconds\":%.6f,\"ops_s\":%.3f,\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f}%n",
            variant, mode, concurrency, count, errors.get(), seconds, success.length / seconds,
            percentile(success, .50), percentile(success, .95), percentile(success, .99));
        if (errors.get() != 0) throw new AssertionError("Failed operations: " + errors.get());
    }

    static double percentile(long[] values, double p) {
        return values[Math.max(0, (int) Math.ceil(values.length * p) - 1)] / 1e6;
    }

    static void runConnections(int concurrency, int count, long[] timings, AtomicInteger errors) {
        Flux.range(0, count).flatMap(i -> Mono.defer(() -> {
            long start = System.nanoTime();
            return FACTORY.create().flatMap(c -> {
                timings[i] = System.nanoTime() - start;
                return c.close();
            }).doOnError(e -> { timings[i] = 0; errors.incrementAndGet(); })
                .onErrorResume(e -> Mono.empty());
        }), concurrency).blockLast(Duration.ofMinutes(3));
    }

    static void runSql(String sql, int column, int concurrency, int count,
        long[] timings, AtomicInteger errors) {
        Flux.range(0, concurrency).flatMap(worker -> Mono.usingWhen(FACTORY.create(), c ->
            Flux.range(0, (count + concurrency - 1) / concurrency)
                .map(n -> n * concurrency + worker).filter(i -> i < count)
                .concatMap(i -> Mono.defer(() -> {
                    long start = System.nanoTime();
                    return c.createStatement(sql).execute()
                        .flatMap(r -> r.map(row -> row.get(column, String.class)))
                        .single().doOnNext(value -> {
                            if (!"50".equals(value)) throw new AssertionError("Unexpected timeout: " + value);
                            timings[i] = System.nanoTime() - start;
                        }).then();
                })).then(), c -> c.close()), concurrency)
            .doOnError(e -> errors.incrementAndGet()).blockLast(Duration.ofMinutes(3));
    }
}
