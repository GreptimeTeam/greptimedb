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

use snafu::ResultExt;
use sqlparser::keywords::Keyword;

use crate::error::{Result, SyntaxSnafu};
use crate::parser::ParserContext;

impl ParserContext<'_> {
    /// Parses MySQL style 'PREPARE stmt_name' into a stmt_name string.
    pub(crate) fn parse_deallocate(&mut self) -> Result<String> {
        self.parser
            .expect_keyword(Keyword::DEALLOCATE)
            .context(SyntaxSnafu)?;
        let stmt_name = self.parser.parse_identifier(false).context(SyntaxSnafu)?;
        Ok(stmt_name.value)
    }
}
