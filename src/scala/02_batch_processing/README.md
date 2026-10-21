# Stage 2 - Batch processing (same data, four Spark APIs)

`rdd/` low-level RDD -> `dataframe/` DataFrame -> `dataset/` typed Dataset -> `sql/` Spark SQL.

`sql/CMUSQLProcessor.scala` writes `/project/batch_results/sql_output`, the input of stages 3, 4 and 6.
