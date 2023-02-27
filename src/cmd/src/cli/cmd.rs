// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::{Error, InvalidReplCommandSnafu, Result};

/// Represents the parsed command from the user (which may be over many lines)
#[derive(Debug, PartialEq)]
pub(crate) enum ReplCommand {
    Help,
    UseDatabase { db_name: String },
    Sql { sql: String },
    Exit,
}

impl TryFrom<&str> for ReplCommand {
    type Error = Error;

    fn try_from(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return InvalidReplCommandSnafu {
                reason: "No command specified".to_string(),
            }
            .fail();
        }

        // If line ends with ';', it must be treated as a complete input.
        // However, the opposite is not true.
        let input_is_completed = input.ends_with(';');

        let input = input.strip_suffix(';').map(|x| x.trim()).unwrap_or(input);
        let lowercase = input.to_lowercase();
        match lowercase.as_str() {
            "help" => Ok(Self::Help),
            "exit" | "quit" => Ok(Self::Exit),
            _ => match input.split_once(' ') {
                Some((maybe_use, database)) if maybe_use.to_lowercase() == "use" => {
                    Ok(Self::UseDatabase {
                        db_name: database.trim().to_string(),
                    })
                }
                // Any valid SQL must contains at least one whitespace.
                Some(_) if input_is_completed => Ok(Self::Sql {
                    sql: input.to_string(),
                }),
                _ => InvalidReplCommandSnafu {
                    reason: format!("unknown command '{input}', maybe input is not completed"),
                }
                .fail(),
            },
        }
    }
}

impl ReplCommand {
    pub fn help() -> &'static str {
        r#"
Available commands (case insensitive):
- 'help': print this help
- 'exit' or 'quit': exit the REPL
- 'use <your database name>': switch to another database/schema context
- Other typed in text will be treated as SQL.
  You can enter new line while typing, just remember to end it with ';'.
"#
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error::InvalidReplCommand;

    #[test]
    fn test_from_str() {
        fn test_ok(s: &str, expected: ReplCommand) {
            let actual: ReplCommand = s.try_into().unwrap();
            assert_eq!(expected, actual, "'{}'", s);
        }

        fn test_err(s: &str) {
            let result: Result<ReplCommand> = s.try_into();
            assert!(matches!(result, Err(InvalidReplCommand { .. })))
        }

        test_err("");
        test_err("  ");
        test_err("\t");

        test_ok("help", ReplCommand::Help);
        test_ok("help", ReplCommand::Help);
        test_ok("  help", ReplCommand::Help);
        test_ok("  help  ", ReplCommand::Help);
        test_ok("  HELP  ", ReplCommand::Help);
        test_ok("  Help;  ", ReplCommand::Help);
        test_ok("  help  ; ", ReplCommand::Help);

        test_ok("exit", ReplCommand::Exit);
        test_ok("exit;", ReplCommand::Exit);
        test_ok("exit ;", ReplCommand::Exit);
        test_ok("EXIT", ReplCommand::Exit);

        test_ok("quit", ReplCommand::Exit);
        test_ok("quit;", ReplCommand::Exit);
        test_ok("quit ;", ReplCommand::Exit);
        test_ok("QUIT", ReplCommand::Exit);

        test_ok(
            "use Foo",
            ReplCommand::UseDatabase {
                db_name: "Foo".to_string(),
            },
        );
        test_ok(
            "  use Foo ;  ",
            ReplCommand::UseDatabase {
                db_name: "Foo".to_string(),
            },
        );
        // ensure that database name is case sensitive
        test_ok(
            "  use FOO ;  ",
            ReplCommand::UseDatabase {
                db_name: "FOO".to_string(),
            },
        );

        // ensure that we aren't messing with capitalization
        test_ok(
            "SELECT * from foo;",
            ReplCommand::Sql {
                sql: "SELECT * from foo".to_string(),
            },
        );
        // Input line (that don't belong to any other cases above) must ends with ';' to make it a valid SQL.
        test_err("insert blah");
        test_ok(
            "insert blah;",
            ReplCommand::Sql {
                sql: "insert blah".to_string(),
            },
        );
    }
}
