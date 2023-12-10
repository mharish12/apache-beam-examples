# With spark

## Links
- [Beam spark documentation](https://beam.apache.org/documentation/runners/spark/)
  - [Portability](https://beam.apache.org/roadmap/portability/)
  - [Metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics) - Uses dropwizard based metrics.
  - [Word Count examples](https://beam.apache.org/get-started/wordcount-example/)

## Reliability and Retries.
For reliability and retries, you may want to add appropriate error-handling logic
in your processing steps, and you can also explore Apache Beam's built-in mechanisms 
for retries and error handling, such as **Retry**, **DeadLetterQueue**, or **Backoff** policies.
Keep in mind that the Spark runner may have its own considerations for fault tolerance 
and retries, so make sure to refer to the documentation for any additional configurations 
or settings specific to the Apache Spark runner.



