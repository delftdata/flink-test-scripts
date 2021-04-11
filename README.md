# flink-test-scripts

A set of bash scripts that repeatedly execute a native example from Flink's code base, kill a task manager, and check whether the job resumes successfully.

The main script is run-recovery-experiment.sh. This script can be used to launch sequences of complex experiments and record the results.

### Graph only vs. external Kafka generators

The script operates in two modes, controlled by a variable (graph_only). In graph_only mode, generators are inside the graph and automatically generate data.
If graph_only mode is set to false, the job attempts to connect to a kafka cluster. This cluster is assumed to be local (launched from the same docker-compose),
unless execution is set to be distributed, in which case it expects a Kubernetes service to represent Kafka and attempts to create topics "benchmark-input" and "benchmark-output".

### Local experiment vs. Distributed Experiment

The LOCAL_EXPERIMENT constant can be used to set whether the script should expect to be executed in Kubernetes or in Docker.
If set to false (distributed), further configuration is necessary by modifying the addresses of kafka, flink and zookeeper used in the script.
Furthermore, the addresses of one or more benchmark data generators must be passed as input parameters to the script. Through SSH, the script will attempt to call a Kafka generator to generate
the specific kind of data this job expects. Thus, these benchmarker nodes must have installed the kafka distribution packaged with this repository.

### Measurements

Two hand-developed tools are used to measure execution with high precision.
The end-to-end latency measurer is used in distributed experiments to obtain the latency of a record calculated from a timestamp generated at record creation time and a timestamp generated at output topic append time.
The throughput measurer uses Kafka watermarks to calculate topic size deltas, and from that calculate throughput.

If the experiment is graph_only, then flinks metrics services are queried instead, though these are not reliable, and only used for internal testing (not for publishable results).