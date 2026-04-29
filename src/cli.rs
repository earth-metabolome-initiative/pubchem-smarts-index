use crate::{errors::invalid_input, errors::DynError, workflow::PubChemIndex};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BuildOptions {
    pub(crate) verbose: bool,
}

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
            let options = parse_build_options(args)?;
            PubChemIndex::new().build_and_publish(options)
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
    eprintln!("  {program} build [--verbose]");
    eprintln!("  {program} query '<SMARTS>'");
}

fn parse_build_options(args: impl IntoIterator<Item = String>) -> Result<BuildOptions, DynError> {
    let mut options = BuildOptions::default();
    for arg in args {
        match arg.as_str() {
            "-v" | "--verbose" => options.verbose = true,
            _ => {
                return Err(invalid_input(format!(
                    "unknown build option: {arg}; expected --verbose"
                ))
                .into());
            }
        }
    }
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::run;
    use super::{parse_build_options, BuildOptions};
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

    #[test]
    fn build_options_accept_verbose_only() -> Result<(), DynError> {
        assert_eq!(
            parse_build_options(string_args(&["--verbose"]))?,
            BuildOptions { verbose: true }
        );
        assert_eq!(
            parse_build_options(string_args(&["-v"]))?,
            BuildOptions { verbose: true }
        );

        let Err(error) = parse_build_options(string_args(&["--bogus"])) else {
            return Err(invalid_data("unknown build option should fail").into());
        };
        assert_invalid_input(&error)?;
        Ok(())
    }
}
