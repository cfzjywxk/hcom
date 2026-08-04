//! Retained `hcom update` command that fails closed for this fork.

#[derive(clap::Parser, Debug)]
#[command(name = "update", about = "Report disabled upstream updates")]
pub struct UpdateArgs {
    /// Retained compatibility flag; upstream checks remain disabled
    #[arg(long)]
    pub check: bool,
}

pub fn cmd_update(_args: &UpdateArgs) -> i32 {
    eprintln!(
        "Error: upstream updates are disabled for this fork; use a fork-owned source checkout."
    );
    1
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn update_args_default() {
        let args = UpdateArgs::try_parse_from(["update"]).unwrap();
        assert!(!args.check);
    }

    #[test]
    fn update_args_check_flag() {
        let args = UpdateArgs::try_parse_from(["update", "--check"]).unwrap();
        assert!(args.check);
    }
}
