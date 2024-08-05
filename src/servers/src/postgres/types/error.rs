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

use common_error::status_code::StatusCode;
use pgwire::error::ErrorInfo;
use snafu::Snafu;
use strum::{AsRefStr, Display, EnumIter, EnumMessage};

#[derive(Display, Debug, PartialEq)]
#[allow(dead_code)]
enum ErrorSeverity {
    #[strum(serialize = "INFO")]
    Info,
    #[strum(serialize = "DEBUG")]
    Debug,
    #[strum(serialize = "NOTICE")]
    Notice,
    #[strum(serialize = "WARNING")]
    Warning,
    #[strum(serialize = "ERROR")]
    Error,
    #[strum(serialize = "FATAL")]
    Fatal,
    #[strum(serialize = "PANIC")]
    Panic,
}

// refer to: https://www.postgresql.org/docs/16/errcodes-appendix.html
#[derive(Snafu, Debug, AsRefStr, EnumIter, EnumMessage)]
pub enum PgErrorCode {
    // === Begin of Class 00 — Successful Completion ===
    /// successful_completion
    #[snafu(display("successful_completion"))]
    Ec00000 = 0,
    // === End of Class 00 — Successful Completion =====

    // === Begin of Class 01 — Warning ===
    /// warning
    #[snafu(display("warning"))]
    Ec01000 = 100,
    // === End of Class 01 — Warning =====

    // === Begin of Class 02 — No Data (this is also a warning class per the SQL standard) ===
    /// no_data
    #[snafu(display("no_data"))]
    Ec02000 = 200,
    // === End of Class 02 — No Data (this is also a warning class per the SQL standard) =====

    // === Begin of Class 03 — SQL Statement Not Yet Complete ===
    /// sql_statement_not_yet_complete
    #[snafu(display("sql_statement_not_yet_complete"))]
    Ec03000 = 300,
    // === End of Class 03 — SQL Statement Not Yet Complete =====

    // === Begin of Class 08 — Connection Exception ===
    /// connection_exception
    #[snafu(display("connection_exception"))]
    Ec08000 = 400,
    // === End of Class 08 — Connection Exception =====

    // === Begin of Class 09 — Triggered Action Exception ===
    /// triggered_action_exception
    #[snafu(display("triggered_action_exception"))]
    Ec09000 = 500,
    // === End of Class 09 — Triggered Action Exception =====

    // === Begin of Class 0A — Feature Not Supported ===
    /// feature_not_supported
    #[snafu(display("feature_not_supported"))]
    Ec0A000 = 600,
    // === End of Class 0A — Feature Not Supported =====

    // === Begin of Class 0B — Invalid Transaction Initiation ===
    /// invalid_transaction_initiation
    #[snafu(display("invalid_transaction_initiation"))]
    Ec0B000 = 700,
    // === End of Class 0B — Invalid Transaction Initiation =====

    // === Begin of Class 0F — Locator Exception ===
    /// locator_exception
    #[snafu(display("locator_exception"))]
    Ec0F000 = 800,
    // === End of Class 0F — Locator Exception =====

    // === Begin of Class 0L — Invalid Grantor ===
    /// invalid_grantor
    #[snafu(display("invalid_grantor"))]
    Ec0L000 = 900,
    // === End of Class 0L — Invalid Grantor =====

    // === Begin of Class 0P — Invalid Role Specification ===
    /// invalid_role_specification
    #[snafu(display("invalid_role_specification"))]
    Ec0P000 = 1000,
    // === End of Class 0P — Invalid Role Specification =====

    // === Begin of Class 0Z — Diagnostics Exception ===
    /// diagnostics_exception
    #[snafu(display("diagnostics_exception"))]
    Ec0Z000 = 1100,
    // === End of Class 0Z — Diagnostics Exception =====

    // === Begin of Class 20 — Case Not Found ===
    /// case_not_found
    #[snafu(display("case_not_found"))]
    Ec20000 = 1200,
    // === End of Class 20 — Case Not Found =====

    // === Begin of Class 21 — Cardinality Violation ===
    /// cardinality_violation
    #[snafu(display("cardinality_violation"))]
    Ec21000 = 1300,
    // === End of Class 21 — Cardinality Violation =====

    // === Begin of Class 22 — Data Exception ===
    /// data_exception
    #[snafu(display("data_exception"))]
    Ec22000 = 1400,
    /// invalid_parameter_value
    #[snafu(display("invalid_parameter_value"))]
    Ec22023 = 1401,
    // === End of Class 22 — Data Exception =====

    // === Begin of Class 23 — Integrity Constraint Violation ===
    /// integrity_constraint_violation
    #[snafu(display("integrity_constraint_violation"))]
    Ec23000 = 1500,
    // === End of Class 23 — Integrity Constraint Violation =====

    // === Begin of Class 24 — Invalid Cursor State ===
    /// invalid_cursor_state
    #[snafu(display("invalid_cursor_state"))]
    Ec24000 = 1600,
    // === End of Class 24 — Invalid Cursor State =====

    // === Begin of Class 25 — Invalid Transaction State ===
    /// invalid_transaction_state
    #[snafu(display("invalid_transaction_state"))]
    Ec25000 = 1700,
    /// read_only_sql_transaction
    #[snafu(display("read_only_sql_transaction"))]
    Ec25006 = 1701,
    // === End of Class 25 — Invalid Transaction State =====

    // === Begin of Class 26 — Invalid SQL Statement Name ===
    /// invalid_sql_statement_name
    #[snafu(display("invalid_sql_statement_name"))]
    Ec26000 = 1800,
    // === End of Class 26 — Invalid SQL Statement Name =====

    // === Begin of Class 27 — Triggered Data Change Violation ===
    /// triggered_data_change_violation
    #[snafu(display("triggered_data_change_violation"))]
    Ec27000 = 1900,
    // === End of Class 27 — Triggered Data Change Violation =====

    // === Begin of Class 28 — Invalid Authorization Specification ===
    /// invalid_authorization_specification
    #[snafu(display("invalid_authorization_specification"))]
    Ec28000 = 2000,
    /// invalid_password
    #[snafu(display("invalid_password"))]
    Ec28P01 = 1901,
    // === End of Class 28 — Invalid Authorization Specification =====

    // === Begin of Class 2B — Dependent Privilege Descriptors Still Exist ===
    /// dependent_privilege_descriptors_still_exist
    #[snafu(display("dependent_privilege_descriptors_still_exist"))]
    Ec2B000 = 2100,
    // === End of Class 2B — Dependent Privilege Descriptors Still Exist =====

    // === Begin of Class 2D — Invalid Transaction Termination ===
    /// invalid_transaction_termination
    #[snafu(display("invalid_transaction_termination"))]
    Ec2D000 = 2200,
    // === End of Class 2D — Invalid Transaction Termination =====

    // === Begin of Class 2F — SQL Routine Exception ===
    /// sql_routine_exception
    #[snafu(display("sql_routine_exception"))]
    Ec2F000 = 2300,
    // === End of Class 2F — SQL Routine Exception =====

    // === Begin of Class 34 — Invalid Cursor Name ===
    /// invalid_cursor_name
    #[snafu(display("invalid_cursor_name"))]
    Ec34000 = 2400,
    // === End of Class 34 — Invalid Cursor Name =====

    // === Begin of Class 38 — External Routine Exception ===
    /// external_routine_exception
    #[snafu(display("external_routine_exception"))]
    Ec38000 = 2500,
    // === End of Class 38 — External Routine Exception =====

    // === Begin of Class 39 — External Routine Invocation Exception ===
    /// external_routine_invocation_exception
    #[snafu(display("external_routine_invocation_exception"))]
    Ec39000 = 2600,
    // === End of Class 39 — External Routine Invocation Exception =====

    // === Begin of Class 3B — Savepoint Exception ===
    /// savepoint_exception
    #[snafu(display("savepoint_exception"))]
    Ec3B000 = 2700,
    // === End of Class 3B — Savepoint Exception =====

    // === Begin of Class 3D — Invalid Catalog Name ===
    /// invalid_catalog_name
    #[snafu(display("invalid_catalog_name"))]
    Ec3D000 = 2800,
    // === End of Class 3D — Invalid Catalog Name =====

    // === Begin of Class 3F — Invalid Schema Name ===
    /// invalid_schema_name
    #[snafu(display("invalid_schema_name"))]
    Ec3F000 = 2900,
    // === End of Class 3F — Invalid Schema Name =====

    // === Begin of Class 40 — Transaction Rollback ===
    /// transaction_rollback
    #[snafu(display("transaction_rollback"))]
    Ec40000 = 3000,
    // === End of Class 40 — Transaction Rollback =====

    // === Begin of Class 42 — Syntax Error or Access Rule Violation ===
    /// syntax_error_or_access_rule_violation
    #[snafu(display("syntax_error_or_access_rule_violation"))]
    Ec42000 = 3100,
    /// syntax_error
    #[snafu(display("syntax_error"))]
    Ec42601 = 3101,
    /// insufficient_privilege
    #[snafu(display("insufficient_privilege"))]
    Ec42501 = 3102,
    /// undefined_column
    #[snafu(display("undefined_column"))]
    Ec42703 = 3103,
    /// undefined_table
    #[snafu(display("undefined_table"))]
    Ec42P01 = 3104,
    /// undefined_object
    #[snafu(display("undefined_object"))]
    Ec42704 = 3105,
    /// duplicate_column
    #[snafu(display("duplicate_column"))]
    Ec42701 = 3106,
    /// duplicate_database
    #[snafu(display("duplicate_database"))]
    Ec42P04 = 3107,
    /// duplicate_table
    #[snafu(display("duplicate_table"))]
    Ec42P07 = 3108,
    /// invalid_prepared_statement_definition
    #[snafu(display("invalid_prepared_statement_definition"))]
    Ec42P14 = 3109,
    // === End of Class 42 — Syntax Error or Access Rule Violation =====

    // === Begin of Class 44 — WITH CHECK OPTION Violation ===
    /// with_check_option_violation
    #[snafu(display("with_check_option_violation"))]
    Ec44000 = 3200,
    // === End of Class 44 — WITH CHECK OPTION Violation =====

    // === Begin of Class 53 — Insufficient Resources ===
    /// insufficient_resources
    #[snafu(display("insufficient_resources"))]
    Ec53000 = 3300,
    // === End of Class 53 — Insufficient Resources =====

    // === Begin of Class 54 — Program Limit Exceeded ===
    /// program_limit_exceeded
    #[snafu(display("program_limit_exceeded"))]
    Ec54000 = 3400,
    // === End of Class 54 — Program Limit Exceeded =====

    // === Begin of Class 55 — Object Not In Prerequisite State ===
    /// object_not_in_prerequisite_state
    #[snafu(display("object_not_in_prerequisite_state"))]
    Ec55000 = 3500,
    // === End of Class 55 — Object Not In Prerequisite State =====

    // === Begin of Class 57 — Operator Intervention ===
    /// operator_intervention
    #[snafu(display("operator_intervention"))]
    Ec57000 = 3600,
    // === End of Class 57 — Operator Intervention =====

    // === Begin of Class 58 — System Error (errors external to PostgreSQL itself) ===
    /// system_error
    #[snafu(display("system_error"))]
    Ec58000 = 3700,
    // === End of Class 58 — System Error (errors external to PostgreSQL itself) =====

    // === Begin of Class 72 — Snapshot Failure ===
    /// snapshot_too_old
    #[snafu(display("snapshot_too_old"))]
    Ec72000 = 3800,
    // === End of Class 72 — Snapshot Failure =====

    // === Begin of Class F0 — Configuration File Error ===
    /// config_file_error
    #[snafu(display("config_file_error"))]
    EcF0000 = 3900,
    // === End of Class F0 — Configuration File Error =====

    // === Begin of Class HV — Foreign Data Wrapper Error (SQL/MED) ===
    /// fdw_error
    #[snafu(display("fdw_error"))]
    EcHV000 = 4000,
    // === End of Class HV — Foreign Data Wrapper Error (SQL/MED) =====

    // === Begin of Class P0 — PL/pgSQL Error ===
    /// plpgsql_error
    #[snafu(display("plpgsql_error"))]
    EcP0000 = 4100,
    // === End of Class P0 — PL/pgSQL Error =====

    // === Begin of Class XX — Internal Error ===
    /// internal_error
    #[snafu(display("internal_error"))]
    EcXX000 = 4200,
    // === End of Class XX — Internal Error =====
}

impl PgErrorCode {
    fn severity(&self) -> ErrorSeverity {
        match self {
            PgErrorCode::Ec00000 => ErrorSeverity::Info,
            PgErrorCode::Ec01000 => ErrorSeverity::Warning,

            PgErrorCode::EcXX000 | PgErrorCode::Ec42P14 | PgErrorCode::Ec22023 => {
                ErrorSeverity::Error
            }
            PgErrorCode::Ec28000 | PgErrorCode::Ec28P01 | PgErrorCode::Ec3D000 => {
                ErrorSeverity::Fatal
            }

            _ => ErrorSeverity::Error,
        }
    }

    fn code(&self) -> String {
        self.as_ref()[2..].to_string()
    }

    pub fn to_err_info(&self, msg: String) -> ErrorInfo {
        ErrorInfo::new(self.severity().to_string(), self.code(), msg)
    }
}

impl From<PgErrorCode> for ErrorInfo {
    fn from(code: PgErrorCode) -> ErrorInfo {
        code.to_err_info(code.to_string())
    }
}

impl From<StatusCode> for PgErrorCode {
    fn from(code: StatusCode) -> PgErrorCode {
        match code {
            // ====== Begin of common status code ==============
            StatusCode::Success => PgErrorCode::Ec00000,
            StatusCode::Unsupported => PgErrorCode::Ec0A000,
            StatusCode::InvalidArguments => PgErrorCode::Ec22023,
            StatusCode::Cancelled => PgErrorCode::Ec57000,

            StatusCode::Unknown
            | StatusCode::Unexpected
            | StatusCode::IllegalState
            | StatusCode::Internal => PgErrorCode::EcXX000,
            // ====== End of common status code ================

            // ====== Begin of SQL & query related status code =========
            StatusCode::InvalidSyntax => PgErrorCode::Ec42601,
            StatusCode::PlanQuery | StatusCode::EngineExecuteQuery => PgErrorCode::EcP0000,
            // ====== End of SQL & query related status code ===========

            // ====== Begin of catalog & flow related status code =====
            StatusCode::TableNotFound | StatusCode::FlowNotFound | StatusCode::RegionNotFound => {
                PgErrorCode::Ec42P01
            }
            StatusCode::TableAlreadyExists
            | StatusCode::FlowAlreadyExists
            | StatusCode::RegionAlreadyExists => PgErrorCode::Ec42P07,

            StatusCode::TableColumnNotFound => PgErrorCode::Ec42703,
            StatusCode::TableColumnExists => PgErrorCode::Ec42701,
            StatusCode::DatabaseNotFound => PgErrorCode::Ec42704,
            StatusCode::DatabaseAlreadyExists => PgErrorCode::Ec42P04,
            StatusCode::RegionReadonly => PgErrorCode::Ec25006,

            StatusCode::RegionNotReady | StatusCode::RegionBusy | StatusCode::TableUnavailable => {
                PgErrorCode::Ec55000
            }
            // ====== End of catalog & flow related status code =======

            // ====== Begin of storage & server related status code =====
            StatusCode::StorageUnavailable | StatusCode::RequestOutdated => PgErrorCode::EcXX000,
            StatusCode::RuntimeResourcesExhausted => PgErrorCode::Ec53000,
            StatusCode::RateLimited => PgErrorCode::Ec54000,
            // ====== End of storage & server related status code =======

            // ====== Begin of auth related status code =====
            StatusCode::UserNotFound
            | StatusCode::UnsupportedPasswordType
            | StatusCode::UserPasswordMismatch => PgErrorCode::Ec28P01,

            StatusCode::AuthHeaderNotFound | StatusCode::InvalidAuthHeader => PgErrorCode::Ec28000,
            StatusCode::AccessDenied | StatusCode::PermissionDenied => PgErrorCode::Ec42501,
            // ====== End of auth related status code =====
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::status_code::StatusCode;
    use strum::{EnumMessage, IntoEnumIterator};

    use super::{ErrorInfo, ErrorSeverity, PgErrorCode};

    #[test]
    fn test_error_severity() {
        // test for ErrorSeverity enum
        assert_eq!("INFO", ErrorSeverity::Info.to_string());
        assert_eq!("DEBUG", ErrorSeverity::Debug.to_string());
        assert_eq!("NOTICE", ErrorSeverity::Notice.to_string());
        assert_eq!("WARNING", ErrorSeverity::Warning.to_string());

        assert_eq!("ERROR", ErrorSeverity::Error.to_string());
        assert_eq!("FATAL", ErrorSeverity::Fatal.to_string());
        assert_eq!("PANIC", ErrorSeverity::Panic.to_string());

        // test for severity method
        for code in PgErrorCode::iter() {
            let name = code.as_ref();
            assert_eq!("Ec", &name[0..2]);

            if name.starts_with("Ec00") {
                assert_eq!(ErrorSeverity::Info, code.severity());
            } else if name.starts_with("Ec01") {
                assert_eq!(ErrorSeverity::Warning, code.severity());
            } else if name.starts_with("Ec28") || name.starts_with("Ec3D") {
                assert_eq!(ErrorSeverity::Fatal, code.severity());
            } else {
                assert_eq!(ErrorSeverity::Error, code.severity());
            }
        }
    }

    #[test]
    fn test_pg_error_code() {
        let code = PgErrorCode::Ec00000;
        assert_eq!("00000", code.code());
        assert_eq!("successful_completion", code.to_string());

        let code = PgErrorCode::Ec01000;
        assert_eq!("01000", code.code());
        assert_eq!("warning", code.to_string());

        // test display is correct
        for code in PgErrorCode::iter() {
            assert_eq!(&code.to_string(), code.get_documentation().unwrap())
        }
    }

    #[test]
    fn test_pg_err_info() {
        let err_info = ErrorInfo::from(PgErrorCode::Ec00000);
        assert_eq!("INFO", err_info.severity);
        assert_eq!("00000", err_info.code);
        assert_eq!("successful_completion", err_info.message);

        let err_info = ErrorInfo::from(PgErrorCode::Ec01000);
        assert_eq!("WARNING", err_info.severity);
        assert_eq!("01000", err_info.code);
        assert_eq!("warning", err_info.message);

        let err_info =
            PgErrorCode::Ec28P01.to_err_info("password authentication failed".to_string());
        assert_eq!("FATAL", err_info.severity);
        assert_eq!("28P01", err_info.code);
        assert_eq!("password authentication failed", err_info.message);

        let err_info =
            PgErrorCode::Ec42P14.to_err_info("invalid_prepared_statement_definition".to_string());
        assert_eq!("ERROR", err_info.severity);
        assert_eq!("42P14", err_info.code);
        assert_eq!("invalid_prepared_statement_definition", err_info.message);
    }

    fn assert_status_code(msg: &str, code: StatusCode) {
        let code = PgErrorCode::from(code);
        assert_eq!(msg, &code.to_string());
    }

    #[test]
    fn test_from_status_code() {
        assert_status_code("successful_completion", StatusCode::Success);
        assert_status_code("invalid_parameter_value", StatusCode::InvalidArguments);
        assert_status_code("undefined_table", StatusCode::TableNotFound);
        assert_status_code("internal_error", StatusCode::Unknown);
        assert_status_code("invalid_password", StatusCode::UserPasswordMismatch);
    }
}
