package lab.m1;
import static lab.m1.Decision.Sample.*;
import java.util.Properties;
public final class DecisionTest {
    public static void main(String[] args) {
        Decision d=new Decision();
        d.step(OK,OK,OK); assert !d.ready && d.started;
        d.step(OK,OK,OK); assert d.ready;
        d.step(FAIL,FAIL,UNKNOWN); d.step(FAIL,FAIL,UNKNOWN);
        assert d.faults[0]==2 && d.faults[1]==0 && d.missing[2]==2 && !d.ready;
        d.step(UNKNOWN,UNKNOWN,OK); assert d.faults[0]==2;
        Properties p=new Properties(); d.save(p); d=Decision.restore(p);
        d.step(OK,OK,OK); assert !d.ready && d.faults[0]==0;
        d.step(OK,OK,OK); assert d.ready;
        d.step(OK,FAIL,OK); d.step(OK,FAIL,OK); assert !d.ready && d.faults[1]==2;
        d.step(UNKNOWN,OK,OK); assert d.faults[1]==2;
        d.step(OK,OK,FAIL); d.step(OK,OK,FAIL); assert d.faults[2]==2 && d.faults[1]==0;
        System.out.println("M1 decision sequence passed");
    }
}
