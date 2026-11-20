package lab.health;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public final class StallAgent {
    private static final String API = "kafka/server/KafkaApis";
    private static final String LOG = "org/apache/kafka/storage/internals/log/LocalLog";
    private static final String HOOKS = "lab/health/StallHooks";
    private static final String PRODUCE_METHOD =
            "(Lkafka/network/RequestChannel$Request;Lorg/apache/kafka/server/common/RequestLocal;)V";
    private static final String APPEND_METHOD = "(JJJLorg/apache/kafka/common/record/MemoryRecords;)V";

    private StallAgent() {}

    public static void premain(String arguments, Instrumentation instrumentation) {
        String command = System.getProperty("sun.java.command", "");
        if (!command.startsWith("kafka.Kafka ")) {
            return;
        }
        instrumentation.addTransformer(new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String name, Class<?> previous,
                                    ProtectionDomain domain, byte[] bytecode) {
                if (!API.equals(name) && !LOG.equals(name)) {
                    return null;
                }
                ClassReader reader = new ClassReader(bytecode);
                ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
                reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
                    @Override
                    public MethodVisitor visitMethod(int access, String method, String descriptor,
                                                     String signature, String[] exceptions) {
                        MethodVisitor original = super.visitMethod(access, method, descriptor, signature, exceptions);
                        if (API.equals(name) && "handleProduceRequest".equals(method) && PRODUCE_METHOD.equals(descriptor)) {
                            return new MethodVisitor(Opcodes.ASM9, original) {
                                @Override
                                public void visitCode() {
                                    super.visitCode();
                                    visitVarInsn(Opcodes.ALOAD, 1);
                                    visitMethodInsn(Opcodes.INVOKESTATIC, HOOKS, "execution",
                                            "(Lkafka/network/RequestChannel$Request;)V", false);
                                }
                            };
                        }
                        if (LOG.equals(name) && "append".equals(method) && APPEND_METHOD.equals(descriptor)) {
                            return new MethodVisitor(Opcodes.ASM9, original) {
                                @Override
                                public void visitCode() {
                                    super.visitCode();
                                    observe("appendEnter");
                                }
                                @Override
                                public void visitInsn(int opcode) {
                                    if (opcode == Opcodes.RETURN) {
                                        observe("appendComplete");
                                    }
                                    super.visitInsn(opcode);
                                }
                                private void observe(String hook) {
                                    visitVarInsn(Opcodes.ALOAD, 0);
                                    visitVarInsn(Opcodes.ALOAD, 7);
                                    visitMethodInsn(Opcodes.INVOKESTATIC, HOOKS, hook,
                                            "(Lorg/apache/kafka/storage/internals/log/LocalLog;"
                                                    + "Lorg/apache/kafka/common/record/MemoryRecords;)V", false);
                                }
                            };
                        }
                        return original;
                    }
                }, 0);
                return writer.toByteArray();
            }
        });
        System.out.println("HEALTH_STALL_AGENT_READY");
    }
}
