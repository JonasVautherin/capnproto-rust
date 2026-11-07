fn main() -> Result<(), Box<dyn std::error::Error>> {
    capnpc::CompilerCommand::new().file("files.capnp").run()?;
    Ok(())
}
