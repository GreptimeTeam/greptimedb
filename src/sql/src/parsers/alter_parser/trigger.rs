use crate::error::Result;
use crate::parser::ParserContext;
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    pub(super) fn parse_alter_trigger(&mut self) -> Result<Statement> {
        // let start = self.start();
        // self.expect_keyword(Keyword::Alter)?;
        // self.expect_keyword(Keyword::Trigger)?;
        //
        // let name = self.parse_object_name()?;
        // let action = self.parse_trigger_action()?;
        //
        // let end = self.end();
        // Ok(Statement::AlterTrigger {
        //     name,
        //     action,
        //     span: start..end,
        // })

        todo!("parse_alter_trigger not implemented yet");
    }
}
