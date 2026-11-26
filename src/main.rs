use std::ffi::OsString;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::process::ExitCode;
use zeus::Store;

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("zeus-local: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[OsString]) -> io::Result<ExitCode> {
    match args {
        [flag] if flag == "--version" => println!("zeus-local {}", env!("CARGO_PKG_VERSION")),
        [path, command, key, value] if command == "put" => {
            Store::open(path)?.put(key.as_bytes(), value.as_bytes())?;
        }
        [path, command, key] if command == "get" => {
            let store = Store::open(path)?;
            let Some(value) = store.get(key.as_bytes())? else {
                eprintln!("Key not found");
                return Ok(ExitCode::FAILURE);
            };
            let mut stdout = io::stdout().lock();
            stdout.write_all(&value)?;
            stdout.write_all(b"\n")?;
        }
        [path, command, key] if command == "delete" => {
            Store::open(path)?.delete(key.as_bytes())?;
        }
        [path, command] if command == "compact" => {
            Store::open(path)?.compact()?;
        }
        [path, command, destination] if command == "checkpoint" => {
            Store::open(path)?.checkpoint(destination)?;
        }
        _ => {
            eprintln!(
                "Usage: zeus-local LOG put KEY VALUE | zeus-local LOG get KEY | zeus-local LOG delete KEY | zeus-local LOG compact | zeus-local LOG checkpoint TABLE"
            );
            return Ok(ExitCode::from(2));
        }
    }
    Ok(ExitCode::SUCCESS)
}
