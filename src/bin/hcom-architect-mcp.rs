fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(error) = hcom::architect::run_component(&args) {
        eprintln!("hcom-architect-mcp: {error:#}");
        std::process::exit(1);
    }
}
