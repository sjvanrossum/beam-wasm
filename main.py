import typing

import apache_beam
from apache_beam.coders import FastPrimitivesCoder, RowCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.schemas import named_tuple_to_schema
from transforms import wasm
from wasmer import wat2wasm

MyStruct = typing.NamedTuple("MyStruct", [("my_field", str)])


def run(argv=None):
    str_coder = FastPrimitivesCoder()
    tup_coder = RowCoder(named_tuple_to_schema(MyStruct))
    wasm_bytes = wat2wasm(
        """
        (module
        (import "env" "output" (func $output (param i32 i32)))
        (memory $memory 2)
        (type $malloc_t (func (param i32) (result i32)))
        (type $process_t (func (param i32 i32)))
        (func $malloc_f (type $malloc_t) (param $sz i32) (result i32)
            i32.const 0
        )
        (func $process_f (type $process_t) (param $ptr i32) (param $sz i32)
            i32.const 0x10000
            local.get $ptr
            local.get $sz
            memory.copy
            i32.const 0x10000
            local.get $sz
            call $output
        )
        (export "memory" (memory $memory))
        (export "process" (func $process_f))
        (export "malloc" (func $malloc_f)))
        """
    )
    with apache_beam.Pipeline(options=PipelineOptions()) as p:
        _ = (
            p
            | "Create str" >> apache_beam.Create(["Hello world!"])
            | "WASM str" >> wasm.WasmTransform(wasm_bytes, str_coder, str_coder)
            | "Print str" >> apache_beam.Map(print)
        )
        _ = (
            p
            | "Create MyStruct" >> apache_beam.Create([MyStruct("Hello world!")])
            | "WASM MyStruct" >> wasm.WasmTransform(wasm_bytes, tup_coder, tup_coder)
            | "Print MyStruct" >> apache_beam.Map(print)
        )


if __name__ == "__main__":
    run()
