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

use std::borrow::Cow;

use rustyline::completion::Completer;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};

use crate::cli::cmd::ReplCommand;

pub(crate) struct RustylineHelper {
    hinter: HistoryHinter,
    highlighter: MatchingBracketHighlighter,
}

impl Default for RustylineHelper {
    fn default() -> Self {
        Self {
            hinter: HistoryHinter {},
            highlighter: MatchingBracketHighlighter::default(),
        }
    }
}

impl rustyline::Helper for RustylineHelper {}

impl Validator for RustylineHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        match ReplCommand::try_from(input) {
            Ok(_) => Ok(ValidationResult::Valid(None)),
            Err(e) => {
                if input.trim_end().ends_with(';') {
                    // If line ends with ';', it HAS to be a valid command.
                    Ok(ValidationResult::Invalid(Some(e.to_string())))
                } else {
                    Ok(ValidationResult::Incomplete)
                }
            }
        }
    }
}

impl Hinter for RustylineHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &rustyline::Context<'_>) -> Option<Self::Hint> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for RustylineHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        self.highlighter.highlight_prompt(prompt, default)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        use nu_ansi_term::Style;
        Cow::Owned(Style::new().dimmed().paint(hint).to_string())
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        completion: rustyline::CompletionType,
    ) -> Cow<'c, str> {
        self.highlighter.highlight_candidate(candidate, completion)
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }
}

impl Completer for RustylineHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        // If there is a hint, use that as the auto-complete when user hits `tab`
        if let Some(hint) = self.hinter.hint(line, pos, ctx) {
            Ok((pos, vec![hint]))
        } else {
            Ok((0, vec![]))
        }
    }
}
