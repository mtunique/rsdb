fn main() {
    tonic_prost_build::compile_protos("proto/rsdb_rpc.proto").expect("compile rsdb_rpc.proto");
    println!("cargo:rerun-if-changed=proto/rsdb_rpc.proto");
}
