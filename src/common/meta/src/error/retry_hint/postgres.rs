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

use common_error::ext::RetryHint;

/// Converts a tokio-postgres error into a conservative retry hint.
pub fn retry_hint_from_postgres_error(error: &tokio_postgres::Error) -> RetryHint {
    if error.is_closed() {
        return RetryHint::Retryable;
    }

    retry_hint_from_postgres_sql_state(error.code())
}

/// Converts a Postgres SQLSTATE into a conservative retry hint.
fn retry_hint_from_postgres_sql_state(
    state: Option<&tokio_postgres::error::SqlState>,
) -> RetryHint {
    use tokio_postgres::error::SqlState;

    let Some(state) = state else {
        return RetryHint::NonRetryable;
    };

    // PostgreSQL SQLSTATE reference:
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    match state {
        // 40001 serialization_failure: concurrent transaction serialization conflict.
        &SqlState::T_R_SERIALIZATION_FAILURE
        // 40P01 deadlock_detected: transaction deadlock.
        | &SqlState::T_R_DEADLOCK_DETECTED
        // 55P03 lock_not_available: lock could not be acquired now.
        | &SqlState::LOCK_NOT_AVAILABLE
        // 53300 too_many_connections: backend connection capacity exhausted.
        | &SqlState::TOO_MANY_CONNECTIONS
        // 57P01 admin_shutdown: server is shutting down by administrator request.
        | &SqlState::ADMIN_SHUTDOWN
        // 57P02 crash_shutdown: server is shutting down after crash.
        | &SqlState::CRASH_SHUTDOWN
        // 57P03 cannot_connect_now: server is not accepting connections now.
        | &SqlState::CANNOT_CONNECT_NOW
        // 08000 connection_exception: generic connection exception.
        | &SqlState::CONNECTION_EXCEPTION
        // 08001 sqlclient_unable_to_establish_sqlconnection: client could not establish connection.
        | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        // 08003 connection_does_not_exist: connection does not exist.
        | &SqlState::CONNECTION_DOES_NOT_EXIST
        // 08004 sqlserver_rejected_establishment_of_sqlconnection: server rejected connection establishment.
        | &SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        // 08006 connection_failure: connection failure.
        | &SqlState::CONNECTION_FAILURE => RetryHint::Retryable,
        _ => RetryHint::NonRetryable,
    }
}

/// Converts a deadpool Postgres pool error into a conservative retry hint.
pub fn retry_hint_from_postgres_pool_error(
    error: &deadpool::managed::PoolError<tokio_postgres::Error>,
) -> RetryHint {
    match error {
        deadpool::managed::PoolError::Timeout(_) => RetryHint::Retryable,
        deadpool::managed::PoolError::Backend(error) => retry_hint_from_postgres_error(error),
        deadpool::managed::PoolError::PostCreateHook(error) => match error {
            deadpool::managed::HookError::Backend(error) => retry_hint_from_postgres_error(error),
            deadpool::managed::HookError::Message(_) => RetryHint::NonRetryable,
        },
        deadpool::managed::PoolError::Closed | deadpool::managed::PoolError::NoRuntimeSpecified => {
            RetryHint::NonRetryable
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::RetryHint;
    use tokio_postgres::error::SqlState;

    use super::*;

    #[test]
    fn test_postgres_sql_state_retry_hint() {
        let retryable_states = [
            &SqlState::T_R_SERIALIZATION_FAILURE,
            &SqlState::T_R_DEADLOCK_DETECTED,
            &SqlState::LOCK_NOT_AVAILABLE,
            &SqlState::TOO_MANY_CONNECTIONS,
            &SqlState::ADMIN_SHUTDOWN,
            &SqlState::CRASH_SHUTDOWN,
            &SqlState::CANNOT_CONNECT_NOW,
            &SqlState::CONNECTION_EXCEPTION,
            &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            &SqlState::CONNECTION_DOES_NOT_EXIST,
            &SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
            &SqlState::CONNECTION_FAILURE,
        ];

        for state in retryable_states {
            assert_eq!(
                retry_hint_from_postgres_sql_state(Some(state)),
                RetryHint::Retryable,
                "SQLSTATE {} should be retryable",
                state.code()
            );
        }

        assert_eq!(
            retry_hint_from_postgres_sql_state(Some(&SqlState::UNDEFINED_TABLE)),
            RetryHint::NonRetryable
        );
        assert_eq!(
            retry_hint_from_postgres_sql_state(None),
            RetryHint::NonRetryable
        );
    }
}
