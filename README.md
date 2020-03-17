# flink-test-scripts

A set of bash scripts that repeatedly execute a native example from Flink's code base, kill a task manager, and check whether the job resumes successfully.

# Assumptions

The scripts assume that:
- Flink's code base and this repository are side by side, that is, they have the same parent folder
- they are invoked from the directory they are located in, i.e. flink-test-scripts
