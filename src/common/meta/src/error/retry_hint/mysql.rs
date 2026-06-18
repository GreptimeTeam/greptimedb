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

use common_error::ext::{RetryHint, retry_hint_from_io_error};

// MySQL error reference:
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
// https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
// MySQL 5.7 error reference:
// https://docs.oracle.com/cd/E17952_01/mysql-errors-5.7-en/server-error-reference.html
// Prefer errno over SQLSTATE because MySQL mixes transient connection failures
// and non-retryable protocol/configuration errors under SQLSTATE class 08.

// ER_CON_COUNT_ERROR, SQLSTATE 08004: Too many connections.
const ER_CON_COUNT_ERROR: u16 = 1040;
// ER_BAD_HOST_ERROR, SQLSTATE 08S01: Can't get hostname for your address.
const ER_BAD_HOST_ERROR: u16 = 1042;
// ER_HANDSHAKE_ERROR, SQLSTATE 08S01: Bad handshake.
// Treat it as non-retryable because it is commonly caused by protocol,
// authentication, TLS, or configuration mismatches.
const ER_HANDSHAKE_ERROR: u16 = 1043;
// ER_UNKNOWN_COM_ERROR, SQLSTATE 08S01: Unknown command.
const ER_UNKNOWN_COM_ERROR: u16 = 1047;
// ER_ACCESS_DENIED_ERROR, SQLSTATE 28000: Access denied for user.
const ER_ACCESS_DENIED_ERROR: u16 = 1045;
// ER_BAD_DB_ERROR, SQLSTATE 42000: Unknown database.
const ER_BAD_DB_ERROR: u16 = 1049;
// ER_SERVER_SHUTDOWN, SQLSTATE 08S01: Server shutdown in progress.
const ER_SERVER_SHUTDOWN: u16 = 1053;
// ER_FORCING_CLOSE, SQLSTATE 08S01: Forcing close of thread.
const ER_FORCING_CLOSE: u16 = 1080;
// ER_DUP_ENTRY, SQLSTATE 23000: Duplicate entry for key.
const ER_DUP_ENTRY: u16 = 1062;
// ER_NO_SUCH_TABLE, SQLSTATE 42S02: Table doesn't exist.
const ER_NO_SUCH_TABLE: u16 = 1146;

// ER_ABORTING_CONNECTION, SQLSTATE 08S01: Aborted connection.
const ER_ABORTING_CONNECTION: u16 = 1152;
// ER_NET_PACKET_TOO_LARGE, SQLSTATE 08S01: Packet bigger than max_allowed_packet.
const ER_NET_PACKET_TOO_LARGE: u16 = 1153;
// ER_NET_READ_ERROR_FROM_PIPE, SQLSTATE 08S01: Read error from connection pipe.
const ER_NET_READ_ERROR_FROM_PIPE: u16 = 1154;
// ER_NET_FCNTL_ERROR, SQLSTATE 08S01: Error from fcntl().
const ER_NET_FCNTL_ERROR: u16 = 1155;
// ER_NET_PACKETS_OUT_OF_ORDER, SQLSTATE 08S01: Packets out of order.
const ER_NET_PACKETS_OUT_OF_ORDER: u16 = 1156;
// ER_NET_UNCOMPRESS_ERROR, SQLSTATE 08S01: Couldn't uncompress communication packet.
const ER_NET_UNCOMPRESS_ERROR: u16 = 1157;
// ER_NET_READ_ERROR, SQLSTATE 08S01: Error reading communication packets.
const ER_NET_READ_ERROR: u16 = 1158;
// ER_NET_READ_INTERRUPTED, SQLSTATE 08S01: Timeout reading communication packets.
const ER_NET_READ_INTERRUPTED: u16 = 1159;
// ER_NET_ERROR_ON_WRITE, SQLSTATE 08S01: Error writing communication packets.
const ER_NET_ERROR_ON_WRITE: u16 = 1160;
// ER_NET_WRITE_INTERRUPTED, SQLSTATE 08S01: Timeout writing communication packets.
const ER_NET_WRITE_INTERRUPTED: u16 = 1161;
// ER_NEW_ABORTING_CONNECTION, SQLSTATE 08S01: Aborted connection.
const ER_NEW_ABORTING_CONNECTION: u16 = 1184;
// ER_MASTER_NET_READ / ER_SOURCE_NET_READ, SQLSTATE 08S01: Net error reading from source.
const ER_MASTER_NET_READ: u16 = 1189;
// ER_MASTER_NET_WRITE / ER_SOURCE_NET_WRITE, SQLSTATE 08S01: Net error writing to source.
const ER_MASTER_NET_WRITE: u16 = 1190;

// MySQL documents ER_LOCK_WAIT_TIMEOUT as SQLSTATE HY000, so classify it by
// the structured errno instead of SQLSTATE.
// ER_TOO_MANY_USER_CONNECTIONS, SQLSTATE 42000: Too many user connections.
const ER_TOO_MANY_USER_CONNECTIONS: u16 = 1203;
// ER_LOCK_WAIT_TIMEOUT, SQLSTATE HY000: Lock wait timeout exceeded; try restarting transaction.
const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
// ER_LOCK_DEADLOCK, SQLSTATE 40001: Deadlock found when trying to get lock; try restarting transaction.
const ER_LOCK_DEADLOCK: u16 = 1213;
// ER_CONNECT_TO_MASTER / ER_CONNECT_TO_SOURCE, SQLSTATE 08S01: Error connecting to source.
const ER_CONNECT_TO_MASTER: u16 = 1218;
// ER_USER_LIMIT_REACHED, SQLSTATE 42000: User limit reached.
const ER_USER_LIMIT_REACHED: u16 = 1226;
// ER_NOT_SUPPORTED_AUTH_MODE, SQLSTATE 08004: Client does not support authentication protocol.
const ER_NOT_SUPPORTED_AUTH_MODE: u16 = 1251;
// ER_NET_OK_PACKET_TOO_LARGE, SQLSTATE 08S01: OK packet too large.
const ER_NET_OK_PACKET_TOO_LARGE: u16 = 3068;

// CR_SERVER_GONE_ERROR, client error: MySQL server has gone away.
const CR_SERVER_GONE_ERROR: u16 = 2006;
// CR_SERVER_LOST, client error: Lost connection to MySQL server during query.
const CR_SERVER_LOST: u16 = 2013;

/// Converts MySQL database error details into a conservative retry hint.
fn retry_hint_from_mysql_database_error(number: Option<u16>, message: &str) -> RetryHint {
    match number {
        Some(
            ER_CON_COUNT_ERROR
            | ER_TOO_MANY_USER_CONNECTIONS
            | self::ER_USER_LIMIT_REACHED
            | ER_BAD_HOST_ERROR
            | ER_SERVER_SHUTDOWN
            | ER_FORCING_CLOSE
            | ER_ABORTING_CONNECTION
            | ER_NET_READ_ERROR_FROM_PIPE
            | ER_NET_FCNTL_ERROR
            | ER_NET_READ_ERROR
            | ER_NET_READ_INTERRUPTED
            | ER_NET_ERROR_ON_WRITE
            | ER_NET_WRITE_INTERRUPTED
            | ER_NEW_ABORTING_CONNECTION
            | ER_MASTER_NET_READ
            | ER_MASTER_NET_WRITE
            | ER_LOCK_WAIT_TIMEOUT
            | ER_LOCK_DEADLOCK
            | ER_CONNECT_TO_MASTER
            | CR_SERVER_GONE_ERROR
            | CR_SERVER_LOST,
        ) => return RetryHint::Retryable,
        // These are explicit reviewed non-retryable MySQL errno values.
        // Retrying the same request usually cannot fix protocol,
        // authentication, payload-size, or schema issues.
        Some(
            ER_HANDSHAKE_ERROR
            | ER_UNKNOWN_COM_ERROR
            | ER_ACCESS_DENIED_ERROR
            | ER_BAD_DB_ERROR
            | ER_DUP_ENTRY
            | ER_NO_SUCH_TABLE
            | ER_NET_PACKET_TOO_LARGE
            | ER_NET_PACKETS_OUT_OF_ORDER
            | ER_NET_UNCOMPRESS_ERROR
            | ER_NOT_SUPPORTED_AUTH_MODE
            | ER_NET_OK_PACKET_TOO_LARGE,
        ) => return RetryHint::NonRetryable,
        _ => {}
    }

    if is_mysql_serialization_database_error(message) {
        RetryHint::Retryable
    } else {
        RetryHint::NonRetryable
    }
}

fn is_mysql_serialization_database_error(message: &str) -> bool {
    matches!(
        message,
        "Deadlock found when trying to get lock; try restarting transaction"
            | "can't serialize access for this transaction"
    )
}

pub fn is_mysql_serialization_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(error) => {
            let mysql_error = error
                .as_error()
                .downcast_ref::<sqlx::mysql::MySqlDatabaseError>();
            matches!(
                mysql_error.map(|error| error.number()),
                Some(ER_LOCK_WAIT_TIMEOUT | ER_LOCK_DEADLOCK)
            ) || is_mysql_serialization_database_error(error.message())
        }
        _ => false,
    }
}

/// Converts a sqlx error into a conservative retry hint.
pub fn retry_hint_from_sqlx_error(error: &sqlx::Error) -> RetryHint {
    match error {
        sqlx::Error::Io(error) => retry_hint_from_io_error(error),
        // SQLx exposes TLS errors as boxed errors and protocol errors as debug
        // strings, so we cannot classify them reliably by structured details.
        // TLS errors are often certificate/configuration failures, while
        // protocol errors may indicate a driver bug or protocol mismatch.
        // Keep them non-retryable to avoid retrying deterministic failures.
        sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => RetryHint::NonRetryable,
        sqlx::Error::PoolTimedOut | sqlx::Error::WorkerCrashed => RetryHint::Retryable,
        sqlx::Error::Database(error) => {
            let mysql_error = error
                .as_error()
                .downcast_ref::<sqlx::mysql::MySqlDatabaseError>();
            retry_hint_from_mysql_database_error(
                mysql_error.map(|error| error.number()),
                error.message(),
            )
        }
        sqlx::Error::Configuration(_)
        | sqlx::Error::InvalidArgument(_)
        | sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Encode(_)
        | sqlx::Error::Decode(_)
        | sqlx::Error::AnyDriverError(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::InvalidSavePointStatement
        | sqlx::Error::BeginFailed => RetryHint::NonRetryable,
        _ => RetryHint::NonRetryable,
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::RetryHint;

    use super::*;

    #[test]
    fn test_mysql_database_error_retry_hint() {
        let retryable_numbers = [
            ER_CON_COUNT_ERROR,
            ER_TOO_MANY_USER_CONNECTIONS,
            ER_USER_LIMIT_REACHED,
            ER_BAD_HOST_ERROR,
            ER_SERVER_SHUTDOWN,
            ER_FORCING_CLOSE,
            ER_ABORTING_CONNECTION,
            ER_NET_READ_ERROR_FROM_PIPE,
            ER_NET_FCNTL_ERROR,
            ER_NET_READ_ERROR,
            ER_NET_READ_INTERRUPTED,
            ER_NET_ERROR_ON_WRITE,
            ER_NET_WRITE_INTERRUPTED,
            ER_NEW_ABORTING_CONNECTION,
            ER_MASTER_NET_READ,
            ER_MASTER_NET_WRITE,
            ER_LOCK_WAIT_TIMEOUT,
            ER_LOCK_DEADLOCK,
            ER_CONNECT_TO_MASTER,
            CR_SERVER_GONE_ERROR,
            CR_SERVER_LOST,
        ];

        for number in retryable_numbers {
            assert_eq!(
                retry_hint_from_mysql_database_error(Some(number), "retryable mysql error"),
                RetryHint::Retryable,
                "errno {number} should be retryable"
            );
        }

        let non_retryable_numbers = [
            ER_HANDSHAKE_ERROR,
            ER_UNKNOWN_COM_ERROR,
            ER_ACCESS_DENIED_ERROR,
            ER_BAD_DB_ERROR,
            ER_DUP_ENTRY,
            ER_NO_SUCH_TABLE,
            ER_NET_PACKET_TOO_LARGE,
            ER_NET_PACKETS_OUT_OF_ORDER,
            ER_NET_UNCOMPRESS_ERROR,
            ER_NOT_SUPPORTED_AUTH_MODE,
            ER_NET_OK_PACKET_TOO_LARGE,
        ];

        for number in non_retryable_numbers {
            assert_eq!(
                retry_hint_from_mysql_database_error(Some(number), "non-retryable mysql error"),
                RetryHint::NonRetryable,
                "errno {number} should be non-retryable"
            );
        }
    }

    #[test]
    fn test_mysql_database_error_message_fallback_retry_hint() {
        assert_eq!(
            retry_hint_from_mysql_database_error(
                None,
                "Deadlock found when trying to get lock; try restarting transaction",
            ),
            RetryHint::Retryable
        );
        assert_eq!(
            retry_hint_from_mysql_database_error(
                None,
                "can't serialize access for this transaction",
            ),
            RetryHint::Retryable
        );
        assert_eq!(
            retry_hint_from_mysql_database_error(None, "unknown mysql error"),
            RetryHint::NonRetryable
        );
        assert_eq!(
            retry_hint_from_mysql_database_error(Some(9999), "unknown mysql error"),
            RetryHint::NonRetryable
        );
    }

    #[test]
    fn test_mysql_serialization_database_error() {
        assert!(is_mysql_serialization_database_error(
            "Deadlock found when trying to get lock; try restarting transaction",
        ));
        assert!(is_mysql_serialization_database_error(
            "can't serialize access for this transaction"
        ));
        assert!(!is_mysql_serialization_database_error("duplicate entry"));
    }
}
