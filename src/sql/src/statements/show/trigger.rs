use std::fmt::{self, Display};

use serde::Serialize;
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::show::ShowKind;

/// SQL structure for `SHOW TRIGGERS`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct ShowTriggers {
    pub kind: ShowKind,
}

impl Display for ShowTriggers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TRIGGERS")?;
        format_kind!(self, f);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::show::trigger::ShowTriggers;
    use crate::statements::show::ShowKind;
    use crate::statements::statement::Statement;

    #[test]
    fn test_show_triggers_display() {
        let show_triggers = ShowTriggers {
            kind: ShowKind::All,
        };
        assert_eq!(show_triggers.to_string(), "SHOW TRIGGERS");

        let sql = "SHOW TRIGGERS LIKE 'test_trigger'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let Statement::ShowTriggers(show_triggers) = &result[0] else {
            panic!("Expected ShowTriggers statement");
        };
        let expected = "SHOW TRIGGERS LIKE 'test_trigger'";
        assert_eq!(show_triggers.to_string(), expected);

        let sql = "SHOW TRIGGERS WHERE name = 'test_trigger'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let Statement::ShowTriggers(show_triggers) = &result[0] else {
            panic!("Expected ShowTriggers statement");
        };
        let expected = "SHOW TRIGGERS WHERE name = 'test_trigger'";
        assert_eq!(show_triggers.to_string(), expected);
    }
}
