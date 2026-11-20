package lab.m1;

import java.util.*;
import java.nio.charset.StandardCharsets;
import kafka.network.RequestChannel;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.storage.internals.log.LocalLog;
import scala.reflect.ClassTag$;

public final class Observation {
    private static final String GEN = UUID.randomUUID().toString();
    private record Pending(long since, Thread thread) {}
    private static final Map<String,Pending> executing = new HashMap<>(), appending = new HashMap<>();
    private static final Map<String,Long> passed = new HashMap<>();
    private static String topic = "";
    private static long seq, progressed, appended, execInvalidUntil, diskInvalidUntil;
    private static final long STALL_NS = 2_000_000_000L;

    public static void start() throws Exception {
        Properties epoch = new Properties();
        epoch.setProperty("generation", GEN);
        Shared.write("epoch", epoch);
        Thread publisher = new Thread(() -> {
            while (true) {
                try {
                    publish(Shared.target());
                    Thread.sleep(250);
                } catch (InterruptedException end) { return; }
                catch (Exception e) {
                    System.err.println("M1 observer: " + e);
                    try { Thread.sleep(250); } catch (InterruptedException end) { return; }
                }
            }
        }, "m1-observer");
        publisher.setDaemon(true);
        publisher.start();
    }

    private static void publish(String selected) throws Exception {
        Properties p = new Properties();
        synchronized (Observation.class) {
            if (!selected.equals(topic)) {
                executing.clear();
                appending.clear();
                passed.clear();
                progressed = appended = execInvalidUntil = diskInvalidUntil = 0;
                topic = selected;
            }
            long now = System.nanoTime();
            passed.values().removeIf(time -> now - time > 120_000_000_000L);
            p.setProperty("execPending", overdue(executing, now, true));
            p.setProperty("diskPending", overdue(appending, now, false));
            p.setProperty("execCount", Long.toString(progressed));
            p.setProperty("diskCount", Long.toString(appended));
            p.setProperty("execValid", Boolean.toString(now >= execInvalidUntil));
            p.setProperty("diskValid", Boolean.toString(now >= diskInvalidUntil));
            p.setProperty("seq", Long.toString(++seq));
        }
        p.setProperty("generation", GEN);
        p.setProperty("topic", selected);
        p.setProperty("time", Long.toString(System.currentTimeMillis()));
        Shared.write("observation", p);
    }

    private static String overdue(Map<String,Pending> pending, long now, boolean execution) {
        String owner = execution ? "kafka.server.KafkaApis" : "org.apache.kafka.storage.internals.log.LocalLog";
        String method = execution ? "handleProduceRequest" : "append";
        var entries = pending.entrySet().iterator();
        while (entries.hasNext()) {
            var entry = entries.next();
            Pending call = entry.getValue();
            if (now - call.since < STALL_NS) continue;
            // A method can unwind through a nested exception without a normal return marker.
            boolean inside = Arrays.stream(call.thread.getStackTrace())
                    .anyMatch(frame -> frame.getClassName().equals(owner) && frame.getMethodName().equals(method));
            if (inside) return entry.getKey();
            entries.remove();
            lost(execution);
        }
        return "";
    }

    private static synchronized void lost(boolean execution) {
        long until = System.nanoTime() + 5_000_000_000L;
        if (execution) execInvalidUntil = until;
        else diskInvalidUntil = until;
    }

    public static void request(RequestChannel.Request request, int stage) {
        String client = request.header().clientId();
        if (!"m1-diagnostic".equals(client) && !"health-validation-workload".equals(client)) return;
        try {
            ProduceRequest body = request.body(ClassTag$.MODULE$.apply(ProduceRequest.class));
            for (var t : body.data().topicData()) for (var part : t.partitionData()) {
                if (part.index() == 0 && part.records() instanceof MemoryRecords records)
                    track(t.name(), records, stage);
            }
        } catch (RuntimeException e) { lost(true); }
    }

    public static void storage(LocalLog log, MemoryRecords records, int stage) {
        try {
            if (log.topicPartition().partition() == 0) track(log.topicPartition().topic(), records, stage);
        } catch (RuntimeException e) { lost(false); }
    }

    private static synchronized void track(String name, MemoryRecords records, int stage) {
        if (!topic.equals(name)) return;
        for (var record : records.records()) {
            var key = record.key();
            if (key == null || key.remaining() != 36) continue;
            String id = StandardCharsets.UTF_8.decode(key.duplicate()).toString();
            if (executing.size() + appending.size() + passed.size() >= 4096) {
                executing.clear(); appending.clear(); passed.clear(); lost(true); lost(false);
            }
            switch (stage) {
                case 0 -> executing.putIfAbsent(id, new Pending(System.nanoTime(),Thread.currentThread()));
                case 1 -> {
                    if (executing.remove(id) != null) {
                        progressed++;
                        passed.put(id,System.nanoTime());
                    }
                }
                case 2 -> {
                    if (passed.remove(id) != null)
                        appending.putIfAbsent(id,new Pending(System.nanoTime(),Thread.currentThread()));
                }
                case 3 -> { if (appending.remove(id) != null) appended++; }
                default -> throw new IllegalArgumentException();
            }
        }
    }
}
