fn main() {
    if let Err(error) = hcom::orchestrator::run_hcomd_service() {
        eprintln!("hcomd: {error:#}");
        std::process::exit(1);
    }
}
