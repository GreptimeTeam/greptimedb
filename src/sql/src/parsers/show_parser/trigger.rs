use crate::error::Result;
use crate::parser::ParserContext;
use crate::statements::show::trigger::ShowTriggers;
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    pub(super) fn parse_show_triggers(&mut self) -> Result<Statement> {
        let kind = self.parse_show_kind()?;
        let show_triggers = ShowTriggers { kind };
        Ok(Statement::ShowTriggers(show_triggers))
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::statements::show::ShowKind;
    use crate::statements::statement::Statement;

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

        // Invalid, since uncorrect keyword `LI`.
        let sql = "LI 'test_trigger'";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_show_triggers().is_err());
    }
}
