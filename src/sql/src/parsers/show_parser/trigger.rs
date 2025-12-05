use snafu::{ResultExt, ensure};

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::show::trigger::{ShowCreateTrigger, ShowTriggers};
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    pub(super) fn parse_show_triggers(&mut self) -> Result<Statement> {
        let kind = self.parse_show_kind()?;
        let show_triggers = ShowTriggers { kind };
        Ok(Statement::ShowTriggers(show_triggers))
    }

    pub(super) fn parse_show_create_trigger(&mut self) -> Result<Statement> {
        let trigger_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a trigger name",
                actual: self.peek_token_as_string(),
            })?;

        let trigger_name = Self::canonicalize_object_name(trigger_name)?;

        ensure!(
            !trigger_name.0.is_empty(),
            error::InvalidSqlSnafu {
                msg: format!(
                    "trigger name should be in the format of <trigger> or <catalog>.<trigger>, got: {}",
                    trigger_name
                ),
            }
        );

        ensure!(
            trigger_name.0.len() <= 2,
            error::InvalidSqlSnafu {
                msg: format!(
                    "trigger name should be in the format of <trigger> or <catalog>.<trigger>, got: {}",
                    trigger_name
                ),
            }
        );

        let show_create_trigger = ShowCreateTrigger { trigger_name };
        Ok(Statement::ShowCreateTrigger(show_create_trigger))
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::statements::show::ShowKind;
    use crate::statements::statement::Statement;

    #[test]
    fn test_parse_show_create_trigger() {
        // Valid, sql: `SHOW CREATE TRIGGER test_trigger`.
        let sql = "test_trigger";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_create_trigger().unwrap();
        let Statement::ShowCreateTrigger(show_create_trigger) = statement else {
            panic!("Expected ShowCreateTrigger statement");
        };
        assert_eq!(show_create_trigger.trigger_name.to_string(), "test_trigger");

        // Valid, sql: SHOW CREATE TRIGGER `TEST_TRIGGER`.
        let sql = "`TEST_TRIGGER`";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_create_trigger().unwrap();
        let Statement::ShowCreateTrigger(show_create_trigger) = statement else {
            panic!("Expected ShowCreateTrigger statement");
        };
        assert_eq!(
            show_create_trigger.trigger_name.to_string(),
            "`TEST_TRIGGER`"
        );

        // Valid, sql: `SHOW CREATE TRIGGER test_catalog.test_trigger`.
        let sql = "test_catalog.test_trigger";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_create_trigger().unwrap();
        let Statement::ShowCreateTrigger(show_create_trigger) = statement else {
            panic!("Expected ShowCreateTrigger statement");
        };
        assert_eq!(
            show_create_trigger.trigger_name.to_string(),
            "test_catalog.test_trigger"
        );

        // Invalid sql, `SHOW CREATE TRIGGER`, since missing trigger name.
        let sql = "";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_show_create_trigger().is_err());

        // Invalid sql, `SHOW CREATE TRIGGER test_catalog.test_schema.test_trigger`,
        // since trigger name does not support schema.
        let sql = "test_catalog.test_schema.test_trigger";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_show_create_trigger().is_err());
    }

    #[test]
    fn test_parse_show_triggers() {
        // Valid, sql: `SHOW TRIGGERS`.
        let sql = "";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_triggers().unwrap();
        let Statement::ShowTriggers(show_triggers) = statement else {
            panic!("Expected ShowTriggers statement");
        };
        assert_eq!(show_triggers.kind, ShowKind::All);

        // Valid, sql: `SHOW TRIGGERS;`.
        let sql = ";";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_triggers().unwrap();
        let Statement::ShowTriggers(show_triggers) = statement else {
            panic!("Expected ShowTriggers statement");
        };
        assert_eq!(show_triggers.kind, ShowKind::All);

        // Valid, sql: `SHOW TRIGGERS LIKE 'test_trigger'`.
        let sql = "LIKE 'test_trigger'";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_triggers().unwrap();
        let Statement::ShowTriggers(show_triggers) = statement else {
            panic!("Expected ShowTriggers statement");
        };
        let ShowKind::Like(like) = show_triggers.kind else {
            panic!("Expected ShowKind::Like");
        };
        assert_eq!(like.value, "test_trigger");

        // Valid, sql: `SHOW TRIGGERS WHERE name = 'test_trigger'`.
        let sql = "WHERE name = 'test_trigger'";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_show_triggers().unwrap();
        let Statement::ShowTriggers(show_triggers) = statement else {
            panic!("Expected ShowTriggers statement");
        };
        let ShowKind::Where(expr) = show_triggers.kind else {
            panic!("Expected ShowKind::Where");
        };
        assert_eq!(expr.to_string(), "name = 'test_trigger'");

        // Invalid, since incorrect keyword `LI`.
        let sql = "LI 'test_trigger'";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_show_triggers().is_err());
    }
}
