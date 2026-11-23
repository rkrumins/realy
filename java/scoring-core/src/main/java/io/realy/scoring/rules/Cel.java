package io.realy.scoring.rules;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import java.util.Map;

/**
 * Shared CEL environment: {@code f: map<string,double>} (features) and {@code event: map<string,dyn>}
 * (see {@link io.realy.scoring.features.EventView}; nested messages are nested maps, repeated fields lists),
 * with the standard macros (has, all, exists, exists_one, map, filter). Identical declarations are used by the Python
 * verifier, so rules evaluate the same in both languages.
 */
public final class Cel {
    private static final CelOptions OPTIONS = CelOptions.current().build();
    // standard macros (has, all, exists, exists_one, map, filter): rules over lists, e.g.
    // event.items.exists(i, i.qty > 2), as Python's CEL evaluates them
    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .setOptions(OPTIONS)
            .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
            .addVar("f", MapType.create(SimpleType.STRING, SimpleType.DOUBLE))
            .addVar("event", MapType.create(SimpleType.STRING, SimpleType.DYN))
            .setResultType(SimpleType.BOOL)
            .build();
    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder().setOptions(OPTIONS).build();
    /** Derived features written as expressions (feature_schema.yaml `expr`): same variables, numeric result. */
    private static final CelCompiler NUMERIC = CelCompilerFactory.standardCelCompilerBuilder()
            .setOptions(OPTIONS)
            .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
            .addVar("f", MapType.create(SimpleType.STRING, SimpleType.DOUBLE))
            .addVar("event", MapType.create(SimpleType.STRING, SimpleType.DYN))
            .setResultType(SimpleType.DYN)       // numbers, or booleans as 1.0 / 0.0 (checked at evaluation)
            .build();

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

    /** A compiled numeric expression (a derived feature). Thread-safe. */
    public static final class Numeric {
        private final String source;
        private final CelRuntime.Program program;

        Numeric(String source, CelRuntime.Program program) {
            this.source = source;
            this.program = program;
        }

        public double eval(Map<String, ?> vars) {
            try {
                Object r = program.eval(vars);
                if (r instanceof Number n) return n.doubleValue();
                if (r instanceof Boolean b) return b ? 1.0 : 0.0;
                throw new IllegalStateException("CEL expression `" + source + "` returned " + r
                        + " (" + (r == null ? "null" : r.getClass().getSimpleName()) + "): a feature must be a number or a bool");
            } catch (CelEvaluationException e) {
                throw new IllegalStateException("CEL evaluation failed for `" + source + "`: " + e.getMessage(), e);
            }
        }

        public String source() {
            return source;
        }
    }

    public static Numeric compileNumeric(String expression) {
        try {
            CelAbstractSyntaxTree ast = NUMERIC.compile(expression).getAst();
            return new Numeric(expression, RUNTIME.createProgram(ast));
        } catch (CelValidationException e) {
            throw new IllegalArgumentException("invalid CEL expression `" + expression + "` (a feature returns a number "
                    + "or a bool, e.g. `!event.cvv_match`): " + e.getMessage(), e);
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("cannot plan CEL `" + expression + "`: " + e.getMessage(), e);
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
