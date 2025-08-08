use std::fmt::Display;

use serde::Serialize;
use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

/// `DROP TRIGGER` statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct DropTrigger {
    trigger_name: ObjectName,
    drop_if_exists: bool,
}

impl DropTrigger {
    pub fn new(trigger_name: ObjectName, if_exists: bool) -> Self {
        Self {
            trigger_name,
            drop_if_exists: if_exists,
        }
    }

    pub fn trigger_name(&self) -> &ObjectName {
        &self.trigger_name
    }

    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP TRIGGER")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let trigger_name = self.trigger_name();
        write!(f, r#" {trigger_name}"#)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;
    use sqlparser::tokenizer::Span;

    use super::*;

    #[test]
    fn test_drop_trigger_display() {
        let ident = Ident {
            value: "my_trigger".to_string(),
            quote_style: None,
            span: Span::empty(),
        };
        let trigger_name = ObjectName::from(vec![ident]);

        let drop_trigger = DropTrigger::new(trigger_name.clone(), true);
        assert_eq!(
            drop_trigger.to_string(),
            "DROP TRIGGER IF EXISTS my_trigger"
        );

        let drop_trigger_no_if_exists = DropTrigger::new(trigger_name, false);
        assert_eq!(
            drop_trigger_no_if_exists.to_string(),
            "DROP TRIGGER my_trigger"
        );
    }
}
