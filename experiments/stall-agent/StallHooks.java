package lab.health;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.network.RequestChannel;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.storage.internals.log.LocalLog;
import scala.reflect.ClassTag$;

public final class StallHooks {
    private static final String PREFIX = "health-validation-";
    private static final String CLIENT_ID = "health-validation-workload";
    private static final Path CONTROL = Path.of("/tmp/health-validation-stall");
    private static final ConcurrentHashMap<String, AtomicInteger> CLAIMED = new ConcurrentHashMap<>();

    private StallHooks() {}

    private record Control(String topic, String stage, long armDeadline, long holdMs, int limit) {}

    public static void execution(RequestChannel.Request request) {
        if (!CLIENT_ID.equals(request.header().clientId())) {
            return;
        }
        try {
            Control control = readControl();
            if (control == null) {
                return;
            }
            ProduceRequest produce = request.body(ClassTag$.MODULE$.apply(ProduceRequest.class));
            for (var topic : produce.data().topicData()) {
                if (!control.topic.equals(topic.name())) {
                    continue;
                }
                for (var partition : topic.partitionData()) {
                    if (partition.index() == 0 && partition.records() instanceof MemoryRecords records) {
                        observe(control, "execution_enter", records);
                    }
                }
            }
        } catch (RuntimeException failure) {
            System.out.println("HEALTH_STALL_HOOK_ERROR execution " + failure.getClass().getSimpleName());
        }
    }

    public static void appendEnter(LocalLog log, MemoryRecords records) {
        observeLog(log, records, "append_enter");
    }

    public static void appendComplete(LocalLog log, MemoryRecords records) {
        observeLog(log, records, "append_complete");
    }

    private static void observeLog(LocalLog log, MemoryRecords records, String stage) {
        try {
            String topic = log.topicPartition().topic();
            if (!topic.startsWith(PREFIX) || log.topicPartition().partition() != 0) {
                return;
            }
            Control control = readControl();
            if (control != null && control.topic.equals(topic)) {
                observe(control, stage, records);
            }
        } catch (RuntimeException failure) {
            System.out.println("HEALTH_STALL_HOOK_ERROR append " + failure.getClass().getSimpleName());
        }
    }

    private static Control readControl() {
        try {
            String[] fields = Files.readString(CONTROL).strip().split(" ");
            if ((fields.length == 3 || fields.length == 5) && fields[0].matches(PREFIX + "[a-z0-9-]+")) {
                long holdMs = fields.length == 5 ? Long.parseLong(fields[3]) : 16_000;
                int limit = fields.length == 5 ? Integer.parseInt(fields[4]) : 1;
                if (holdMs >= 12_000 && holdMs <= 90_000 && limit >= 1 && limit <= 3) {
                    return new Control(fields[0], fields[1], Long.parseLong(fields[2]), holdMs, limit);
                }
            }
        } catch (IOException | NumberFormatException ignored) {
            // The marker is absent during normal operation.
        }
        return null;
    }

    private static void observe(Control control, String stage, MemoryRecords records) {
        List<String> ids = new ArrayList<>();
        for (var record : records.records()) {
            if (record.key() != null) {
                String id = StandardCharsets.UTF_8.decode(record.key().duplicate()).toString();
                if (id.matches("[0-9a-f-]{36}")) {
                    ids.add(id);
                    event(control.topic, stage, id);
                }
            }
        }
        if (ids.size() == 1 && stage.equals(control.stage) && System.currentTimeMillis() <= control.armDeadline
                && claim(control)) {
            String id = ids.get(0);
            event(control.topic, "stall_enter", id);
            try {
                Thread.sleep(control.holdMs);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                event(control.topic, "stall_release", id);
            }
        }
    }

    private static boolean claim(Control control) {
        AtomicInteger claimed = CLAIMED.computeIfAbsent(control.topic, ignored -> new AtomicInteger());
        return claimed.getAndUpdate(count -> Math.min(count + 1, control.limit)) < control.limit;
    }

    private static void event(String topic, String stage, String id) {
        System.out.println("HEALTH_STALL\t" + System.currentTimeMillis() + "\t" + topic + "\t" + stage + "\t" + id);
    }
}
