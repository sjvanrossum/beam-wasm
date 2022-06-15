import setuptools

setuptools.setup(
    name="beam-wasm",
    version="0.0.1",
    install_requires=[
        "apache_beam==2.39.0",
        "wasmer==1.1.0",
        "wasmer_compiler_cranelift==1.1.0",
        "wasmer_compiler_llvm==1.1.0",
        "wasmer_compiler_singlepass==1.1.0",
    ],
    packages=setuptools.find_packages(),
)
