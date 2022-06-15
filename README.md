# beam-wasm
## WebAssembly in Apache Beam
The pipeline and transform in this repository showcase the minimal requirements to call a Wasm module from a DoFn using Wasmer's WebAssembly runtime for Python.
The provided module has a stub implementation for malloc, a real implementation of malloc would be necessary to reserve memory in the guest environment's linear memory buffer to send an encoded input element. The module expects an external function to be declared to communicate the offset and size of the transformed (in this case, a memcpy from page 1 to page 2) and encoded output element.
## Running the example
Create and activate a virtual environment, install dependencies with pip and run `python -m main` to execute the pipeline using the DirectRunner.