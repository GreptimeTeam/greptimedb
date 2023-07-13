/// Time value, represents the elapsed time since midnight in the unit of `TimeUnit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Time {
    val: i64,
    unit: TimeUnit,
}
