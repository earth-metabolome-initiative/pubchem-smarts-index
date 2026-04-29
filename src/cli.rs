use crate::{errors::invalid_input, errors::DynError, workflow::PubChemIndex};

pub(crate) fn run(args: impl IntoIterator<Item = String>) -> Result<(), DynError> {
    let mut args = args.into_iter();
    let program = args.next().unwrap_or_else(|| "pubchem-smarts-index".into());
    match args.next().as_deref() {
        None | Some("help" | "-h" | "--help") => {
            if args.next().is_some() {
                return Err(invalid_input("help accepts no arguments").into());
            }
            print_usage(&program);
            Ok(())
        }
        Some("build") => {
            if args.next().is_some() {
                return Err(invalid_input("build accepts no arguments").into());
            }
            PubChemIndex::new().build_and_publish()
        }
        Some("query") => {
            let smarts = args
                .next()
                .ok_or_else(|| invalid_input("query requires a SMARTS argument"))?;
            if args.next().is_some() {
                return Err(invalid_input("query accepts exactly one SMARTS argument").into());
            }
            PubChemIndex::new().query_smarts(&smarts)
        }
        Some(command) => {
            print_usage(&program);
            Err(invalid_input(format!("unknown command: {command}")).into())
        }
    }
}

fn print_usage(program: &str) {
    eprintln!("Usage:");
    eprintln!("  {program} build");
    eprintln!("  {program} query '<SMARTS>'");
}

#[cfg(test)]
mod tests {
    use super::run;
    use crate::errors::{invalid_data, DynError};
    use std::io::{Error as IoError, ErrorKind};

    fn string_args(args: &[&str]) -> Vec<String> {
        args.iter().map(ToString::to_string).collect()
    }

    fn assert_invalid_input(error: &DynError) -> Result<(), DynError> {
        let Some(error) = error.downcast_ref::<IoError>() else {
            return Err(invalid_data("expected std::io::Error").into());
        };
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn help_succeeds_but_invalid_commands_fail() -> Result<(), DynError> {
        run(string_args(&["pubchem-smarts-index", "help"]))?;
        run(string_args(&["pubchem-smarts-index", "--help"]))?;

        let Err(error) = run(string_args(&["pubchem-smarts-index", "bogus"])) else {
            return Err(invalid_data("unknown command should fail").into());
        };
        assert_invalid_input(&error)?;

        let Err(error) = run(string_args(&["pubchem-smarts-index", "build", "extra"])) else {
            return Err(invalid_data("build should reject extra arguments").into());
        };
        assert_invalid_input(&error)?;
        Ok(())
    }
}
