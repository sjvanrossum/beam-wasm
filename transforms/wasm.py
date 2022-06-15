from enum import Enum, auto

import apache_beam
from apache_beam.coders.coders import FastCoder
from wasmer import (
    engine,
    Function,
    FunctionType,
    ImportObject,
    Instance,
    Module,
    Store,
    Type,
)


class Engine(Enum):
    UNIVERSAL = auto()
    DYLIB = auto()


class Compiler(Enum):
    CRANELIFT = auto()
    LLVM = auto()
    SINGLEPASS = auto()


class WasmDoFn(apache_beam.DoFn):
    _module_bytes: bytes
    _input_coder: FastCoder
    _output_coder: FastCoder
    _module: Module
    _imports: ImportObject
    _ptr: int
    _sz: int

    def __init__(
        self,
        module_bytes: bytes,
        input_coder: FastCoder,
        output_coder: FastCoder,
        compiler: Compiler = Compiler.CRANELIFT,
        engine: Engine = Engine.UNIVERSAL,
    ):
        self._module_bytes = module_bytes
        self._input_coder = input_coder
        self._output_coder = output_coder
        self._compiler = compiler
        self._engine = engine

    def output(self, ptr: int, sz: int):
        self._ptr = ptr
        self._sz = sz

    def setup(self):
        if self._compiler is Compiler.CRANELIFT:
            from wasmer_compiler_cranelift import Compiler as c
        elif self._compiler is Compiler.LLVM:
            from wasmer_compiler_llvm import Compiler as c
        elif self._compiler is Compiler.SINGLEPASS:
            from wasmer_compiler_singlepass import Compiler as c
        else:
            c = None

        if self._engine is Engine.UNIVERSAL:
            e = engine.Universal(c)
        elif self._engine is Engine.DYLIB:
            e = engine.Dylib(c)
        else:
            e = None

        s = Store(e)
        self._module = Module(s, self._module_bytes)
        self._imports = ImportObject()
        self._imports.register(
            "env",
            {
                "output": Function(
                    s, self.output, FunctionType([Type.I32, Type.I32], [])
                ),
            },
        )

    def process(self, element):
        instance = Instance(self._module, self._imports)
        sz = self._input_coder.estimate_size(element)
        ptr = instance.exports.malloc(sz)
        memoryview(instance.exports.memory.buffer)[
            ptr : ptr + sz
        ] = self._input_coder.encode(element)
        instance.exports.process(ptr, sz)
        yield self._output_coder.decode(
            bytes(
                memoryview(instance.exports.memory.buffer)[
                    self._ptr : self._ptr + self._sz
                ]
            )
        )


class WasmTransform(apache_beam.PTransform):
    _fn: WasmDoFn

    def __init__(self, *args, **kwargs):
        self._fn = WasmDoFn(*args, **kwargs)

    def expand(self, pcoll):
        return pcoll | apache_beam.ParDo(self._fn)
