searchState.loadedDescShard("common_recordbatch", 0, "A two-dimensional batch of column-oriented data with a …\nTrait for a <code>Stream</code> of <code>RecordBatch</code>es that can be passed …\nEmptyRecordBatchStream can be used to create a …\nA two-dimensional batch of column-oriented data with a …\nAdapt a Stream of RecordBatch to a RecordBatchStream.\nGet a reference to a column’s array by index.\nGet a reference to a column’s array by name.\nGet a reference to all columns in the record batch.\nError of record batch.\nUtil record batch stream wrapper that can perform precise …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the total number of bytes of memory occupied …\nGet a reference to a column’s array by name.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCreate an empty RecordBatchStream\nCreates a RecordBatchStreamWrapper without output ordering …\nCreates a new empty <code>RecordBatch</code>.\nNormalize a semi-structured <code>RecordBatch</code> into a flat table.\nReturns the number of columns in the record batch.\nReturns the number of rows in each column.\nProjects the schema onto the specified columns\nRemove column by index and return it.\nReturns the <code>Schema</code> of the record batch.\nSchema wrapped by Arc\nReturns a reference to the <code>Schema</code> of the record batch.\nReturn a new RecordBatch where each column is sliced …\nCreate a <code>RecordBatch</code> from an iterable list of pairs of the …\nCreate a <code>RecordBatch</code> from an iterable list of tuples of the\nCreates a <code>RecordBatch</code> from a schema and columns.\nCreates a <code>RecordBatch</code> from a schema and columns, with …\nOverride the schema of this <code>RecordBatch</code>\nGreptime SendableRecordBatchStream -&gt; DataFusion …\nAn ExecutionPlanVisitor to collect metrics from a …\nJson encoded metrics. Contains metric from a whole plan …\n<code>RecordBatchMetrics</code> carrys metrics value from datanode to …\nDataFusion SendableRecordBatchStream -&gt; Greptime …\nCasts the <code>RecordBatch</code>es of <code>stream</code> against the <code>output_schema</code>…\nCPU consumption in nanoseconds\nDisplay plan and metrics in verbose mode.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nThe level of the plan, starts from 0\nMemory used by the plan in bytes\nAn ordered key-value list of metrics. Key is metric label …\nAggregated plan-level metrics. Resolved after an …\nReturns a single-line summary of the root of the plan. If …\nThe plan name\nAn ordered list of plan metrics, from top to bottom in …\nSet the verbose mode for displaying plan and metrics.\nA cursor on RecordBatchStream that fetches data batch by …\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nTake <code>size</code> of row from the <code>RecordBatchStream</code> and create a …\nSNAFU context selector for the <code>Error::ArrowCompute</code> variant\nSNAFU context selector for the <code>Error::CastVector</code> variant\nSNAFU context selector for the <code>Error::ColumnNotExists</code> …\nSNAFU context selector for the <code>Error::CreateRecordBatches</code> …\nSNAFU context selector for the <code>Error::DataTypes</code> variant\nSNAFU context selector for the <code>Error::EmptyStream</code> variant\nContains the error value\nSNAFU context selector for the <code>Error::External</code> variant\nSNAFU context selector for the <code>Error::Format</code> variant\nSNAFU context selector for the <code>Error::NewDfRecordBatch</code> …\nContains the success value\nSNAFU context selector for the <code>Error::PhysicalExpr</code> variant\nSNAFU context selector for the …\nSNAFU context selector for the …\nSNAFU context selector for the <code>Error::SchemaConversion</code> …\nSNAFU context selector for the <code>Error::SchemaNotMatch</code> …\nSNAFU context selector for the <code>Error::StreamTimeout</code> variant\nSNAFU context selector for the <code>Error::ToArrowScalar</code> variant\nSNAFU context selector for the <code>Error::UnsupportedOperation</code> …\nConsume the selector and return the associated error\nConsume the selector and return the associated error\nConsume the selector and return the associated error\nConsume the selector and return the associated error\nConsume the selector and return the associated error\nConsume the selector and return the associated error\nConsume the selector and return a <code>Result</code> with the …\nConsume the selector and return a <code>Result</code> with the …\nConsume the selector and return a <code>Result</code> with the …\nConsume the selector and return a <code>Result</code> with the …\nConsume the selector and return a <code>Result</code> with the …\nConsume the selector and return a <code>Result</code> with the …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nAn inplace expr evaluator for simple filter. Only support\nEvaluate the predicate on the input RecordBatch, and …\nGet the name of the referenced column.\nName of the referenced column.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nThe literal value.\nOnly used when the operator is <code>Or</code>-chain.\nBuilds a regex pattern from a scalar value and operator. …\nThe operator.\nPre-compiled regex. Only used when the operator is regex …\nWhether the regex is negative.\nThe same as arrow regexp_is_match_scalar() with …\nA two-dimensional batch of column-oriented data with a …\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nmerge multiple recordbatch into a single\nCreate a new <code>RecordBatch</code> from <code>schema</code> and <code>columns</code>.\nCreate an empty <code>RecordBatch</code> from <code>schema</code>.\nCreate an empty <code>RecordBatch</code> from <code>schema</code> with <code>num_rows</code>.\nPretty display this record batch like a table\nCreate an iterator to traverse the data by row\nReturn a slice record batch starts from offset, with len …\nCreate a new <code>RecordBatch</code> from <code>schema</code> and <code>df_record_batch</code>.\nA stream that chains multiple streams into a single stream.\nCollect all the items from the stream into a vector of …\nCollect all the items from the stream into RecordBatches.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.")