package lab.m1;
import java.util.Properties;

final class Decision {
    enum Sample { OK, FAIL, UNKNOWN }
    static final String[] NAMES = {"exec","disk","isr"};
    final int[] faults = new int[3], missing = new int[3];
    int recovery;
    boolean started, ready;
    long round;
    void step(Sample... samples) {
        boolean healthy = true;
        boolean withdraw = false;
        for (int i=0;i<3;i++) {
            Sample s = samples[i];
            missing[i] = s == Sample.UNKNOWN ? Math.min(2,missing[i]+1) : 0;
            boolean assess = s != Sample.UNKNOWN && (i != 1 || samples[0] == Sample.OK);
            if (assess) faults[i] = s == Sample.FAIL ? Math.min(2,faults[i]+1) : 0;
            else if (faults[i] < 2) faults[i] = 0;
            healthy &= s == Sample.OK;
            withdraw |= faults[i] == 2 || missing[i] == 2;
        }
        started |= samples[0] == Sample.OK;
        recovery = healthy ? Math.min(2,recovery+1) : 0;
        if (recovery == 2) ready=true;
        if (withdraw) ready=false;
        round++;
    }
    void save(Properties p) {
        for (int i=0;i<3;i++) {
            p.setProperty(NAMES[i]+"Fault", ""+faults[i]);
            p.setProperty(NAMES[i]+"Missing", ""+missing[i]);
        }
        p.setProperty("ready",""+ready); p.setProperty("started",""+started);
        p.setProperty("round",""+round); p.setProperty("recovery",""+recovery);
    }
    static Decision restore(Properties p) {
        Decision d=new Decision();
        for (int i=0;i<3;i++) {
            d.faults[i]=Integer.parseInt(p.getProperty(NAMES[i]+"Fault"));
            d.missing[i]=Integer.parseInt(p.getProperty(NAMES[i]+"Missing"));
        }
        d.ready=Boolean.parseBoolean(p.getProperty("ready")); d.started=Boolean.parseBoolean(p.getProperty("started"));
        d.recovery=Integer.parseInt(p.getProperty("recovery")); d.round=Long.parseLong(p.getProperty("round"));
        return d;
    }
}
