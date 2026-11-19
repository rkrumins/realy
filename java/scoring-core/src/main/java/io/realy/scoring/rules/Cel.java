package io.realy.scoring.rules;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import java.util.Map;

/**
 * Shared CEL environment: {@code f: map<string,double>} (features) and {@code event: map<string,dyn>}
 * (see {@link io.realy.scoring.features.EventView}). Identical declarations are used by the Python
 * verifier, so rules evaluate the same in both languages.
 */
public final class Cel {
    private static final CelOptions OPTIONS = CelOptions.current().build();
    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .setOptions(OPTIONS)
            .addVar("f", MapType.create(SimpleType.STRING, SimpleType.DOUBLE))
            .addVar("event", MapType.create(SimpleType.STRING, SimpleType.DYN))
            .setResultType(SimpleType.BOOL)
            .build();
    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder().setOptions(OPTIONS).build();

    private Cel() {}

    /** A compiled boolean predicate. Thread-safe. */
    public static final class Predicate {
        private final String source;
        private final CelRuntime.Program program;

        Predicate(String source, CelRuntime.Program program) {
            this.source = source;
            this.program = program;
        }

        public boolean test(Map<String, ?> vars) {
            try {
                return Boolean.TRUE.equals(program.eval(vars));
            } catch (CelEvaluationException e) {
                throw new IllegalStateException("CEL evaluation failed for `" + source + "`: " + e.getMessage(), e);
            }
        }

        public String source() {
            return source;
        }
    }

    public static Predicate compile(String expression) {
        try {
            CelAbstractSyntaxTree ast = COMPILER.compile(expression).getAst();
            return new Predicate(expression, RUNTIME.createProgram(ast));
        } catch (CelValidationException e) {
            throw new IllegalArgumentException("invalid CEL `" + expression + "`: " + e.getMessage(), e);
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("cannot plan CEL `" + expression + "`: " + e.getMessage(), e);
        }
    }
}
