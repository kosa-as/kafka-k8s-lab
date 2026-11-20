package lab.m1;

import java.lang.instrument.*;
import java.security.ProtectionDomain;
import org.objectweb.asm.*;

public final class ObserverAgent {
    public static void premain(String arg, Instrumentation inst) throws Exception {
        if (!System.getProperty("sun.java.command","").startsWith("kafka.Kafka ")) return;

        inst.addTransformer(new ClassFileTransformer() {
            @Override public byte[] transform(ClassLoader loader, String name, Class<?> old,
                                              ProtectionDomain domain, byte[] bytes) {
                boolean isKafkaApi = "kafka/server/KafkaApis".equals(name);
                if (!isKafkaApi && !"org/apache/kafka/storage/internals/log/LocalLog".equals(name)) return null;
                ClassReader reader = new ClassReader(bytes);
                ClassWriter writer = new ClassWriter(reader,ClassWriter.COMPUTE_MAXS);
                reader.accept(new ClassVisitor(Opcodes.ASM9,writer) {
                    @Override public MethodVisitor visitMethod(int flags,String method,String desc,String sig,String[] ex) {
                        MethodVisitor next = super.visitMethod(flags,method,desc,sig,ex);
                        if (isKafkaApi && method.equals("handleProduceRequest") && desc.equals("(Lkafka/network/RequestChannel$Request;Lorg/apache/kafka/server/common/RequestLocal;)V")) {
                            return new MethodVisitor(Opcodes.ASM9,next) {
                                void mark(int stage) {
                                    mv.visitVarInsn(Opcodes.ALOAD,1);
                                    mv.visitInsn(Opcodes.ICONST_0 + stage);
                                    mv.visitMethodInsn(Opcodes.INVOKESTATIC,"lab/m1/Observation","request",
                                            "(Lkafka/network/RequestChannel$Request;I)V",false);
                                }
                                @Override public void visitCode() { super.visitCode(); mark(0); }
                                @Override public void visitMethodInsn(int op,String owner,String n,String d,boolean itf) {
                                    super.visitMethodInsn(op,owner,n,d,itf);
                                    if (owner.equals("kafka/network/RequestChannel$Request") && n.equals("body")) mark(1);
                                }
                            };
                        }
                        if (!isKafkaApi && method.equals("append") && desc.equals("(JJJLorg/apache/kafka/common/record/MemoryRecords;)V")) {
                            return new MethodVisitor(Opcodes.ASM9,next) {
                                void mark(int stage) {
                                    mv.visitVarInsn(Opcodes.ALOAD,0); mv.visitVarInsn(Opcodes.ALOAD,7);
                                    mv.visitInsn(Opcodes.ICONST_0 + stage);
                                    mv.visitMethodInsn(Opcodes.INVOKESTATIC,"lab/m1/Observation","storage",
                                            "(Lorg/apache/kafka/storage/internals/log/LocalLog;Lorg/apache/kafka/common/record/MemoryRecords;I)V",false);
                                }
                                @Override public void visitCode() { super.visitCode(); mark(2); }
                                @Override public void visitInsn(int op) { if (op==Opcodes.RETURN) mark(3); super.visitInsn(op); }
                            };
                        }
                        return next;
                    }
                },0);
                return writer.toByteArray();
            }
        });
        Observation.start();
    }
}
