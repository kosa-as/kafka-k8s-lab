package lab.m1;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;
import static lab.m1.Decision.Sample.*;

public final class Sidecar {
    record Role(int leader, Set<Integer> replicas, Set<Integer> isr, int min) {}
    private final Admin admin;
    private final KafkaProducer<String,String> producer;
    private final int local = Integer.parseInt(System.getenv("M1_BROKER_ID"));
    private Decision state = new Decision();
    private String generation="", target="";
    private long seq=-1, exec=-1, disk=-1;

    Sidecar() {
        Properties p=new Properties();
        p.put("bootstrap.servers","127.0.0.1:9092");
        p.put("request.timeout.ms","1000"); p.put("default.api.timeout.ms","1200");
        admin=Admin.create(p);
        p.put("client.id","m1-diagnostic"); p.put("acks","1"); p.put("enable.idempotence","false");
        p.put("retries","0"); p.put("max.block.ms","300"); p.put("delivery.timeout.ms","1500");
        p.put("key.serializer",StringSerializer.class.getName()); p.put("value.serializer",StringSerializer.class.getName());
        producer=new KafkaProducer<>(p);
    }
    Role role(String name) {
        try {
            ConfigResource cr=new ConfigResource(ConfigResource.Type.TOPIC,name);
            var description=admin.describeTopics(List.of(name)).topicNameValues().get(name);
            var config=admin.describeConfigs(List.of(cr)).values().get(cr);
            var p=description.get(1200,TimeUnit.MILLISECONDS).partitions().stream()
                    .filter(x->x.partition()==0).findFirst().orElseThrow();
            Set<Integer> replicas=new TreeSet<>(), isr=new TreeSet<>();
            p.replicas().forEach(n->replicas.add(n.id())); p.isr().forEach(n->isr.add(n.id()));
            int min=Integer.parseInt(config.get(1200,TimeUnit.MILLISECONDS).get("min.insync.replicas").value());
            return new Role(p.leader().id(),replicas,isr,min);
        } catch (Exception e) { return null; }
    }
    void reset(String gen,String topic) {
        generation=gen; target=topic; state=new Decision(); seq=exec=disk=-1;
        try {
            Properties p=Shared.read("decision");
            if (gen.equals(p.getProperty("generation")) && topic.equals(p.getProperty("topic"))) {
                state=Decision.restore(p);
                seq=Shared.number(p,"seq"); exec=Shared.number(p,"execCount"); disk=Shared.number(p,"diskCount");
            }
        } catch (Exception ignored) { state=new Decision(); }
    }
    void round() throws Exception {
        String name=Shared.target(), gen=Shared.read("epoch").getProperty("generation");
        if (!gen.equals(generation) || !name.equals(target)) reset(gen,name);
        Role before=role(name);
        // The workload fixture selects ordinary UUID keys. Diagnostics have a separate key space.
        String id="m"+UUID.randomUUID().toString().substring(1);
        if (before!=null && before.leader==local) {
            try { producer.send(new ProducerRecord<>(name,0,id,"m1 diagnostic")); }
            catch (RuntimeException ignored) { /* Broker evidence determines validity. */ }
        }
        Thread.sleep(1100);
        Role after=role(name);
        Decision.Sample es=UNKNOWN, ds=UNKNOWN, rs=UNKNOWN;
        Properties observation=new Properties();
        try { observation=Shared.read("observation"); } catch (java.io.IOException ignored) {}
        boolean stable=before!=null && before.equals(after);
        if (stable) rs=after.leader!=local || after.isr.size()>=Math.min(after.min,after.replicas.size()) ? OK : FAIL;
        long age=System.currentTimeMillis()-Shared.number(observation,"time");
        long next=Shared.number(observation,"seq"), ec=Shared.number(observation,"execCount"), dc=Shared.number(observation,"diskCount");
        if (gen.equals(observation.getProperty("generation")) && name.equals(observation.getProperty("topic"))
                && next>seq && age>=0 && age<=1500) {
            if (Boolean.parseBoolean(observation.getProperty("execValid")))
                es=!observation.getProperty("execPending","").isEmpty() ? FAIL : exec>=0 && ec>exec ? OK : UNKNOWN;
            if (stable && after.leader==local && Boolean.parseBoolean(observation.getProperty("diskValid")))
                ds=!observation.getProperty("diskPending","").isEmpty() ? FAIL : disk>=0 && dc>disk ? OK : UNKNOWN;
            seq=next; exec=ec; disk=dc;
        }
        if (!gen.equals(Shared.read("epoch").getProperty("generation")) || !name.equals(Shared.target())) return;
        state.step(es,ds,rs);
        Properties p=new Properties();
        state.save(p);
        p.setProperty("schema","1"); p.setProperty("topic",name); p.setProperty("generation",gen);
        p.setProperty("time",""+System.currentTimeMillis()); p.setProperty("seq",""+seq);
        p.setProperty("execCount",""+exec); p.setProperty("diskCount",""+disk);
        p.setProperty("exec",""+es); p.setProperty("disk",""+ds); p.setProperty("isr",""+rs);
        p.setProperty("diagId",id); p.setProperty("execPending",observation.getProperty("execPending",""));
        p.setProperty("diskPending",observation.getProperty("diskPending",""));
        p.setProperty("metadata",stable ? after.toString() : "UNKNOWN");
        Shared.write("decision",p);
        System.out.println("M1_ROUND "+p);
    }
    public static void main(String[] args) throws Exception {
        Sidecar monitor=new Sidecar();
        while (true) {
            try { monitor.round(); }
            catch (Exception e) { System.err.println("M1 round unavailable: "+e); Thread.sleep(500); }
        }
    }
}
