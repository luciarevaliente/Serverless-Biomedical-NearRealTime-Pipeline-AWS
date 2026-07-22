# Experimental Methodology

> Methodology used to evaluate the performance, scalability and operational characteristics of the platform.

---

## Table of Contents

- [Objectives](#objectives)
- [Research Questions](#research-questions)
- [Experimental Environment](#experimental-environment)
- [Infrastructure Configuration](#infrastructure-configuration)
- [Workloads](#workloads)
- [Service Level Objectives (SLOs)](#service-level-objectives-slos)
- [Experimental Scenarios](#experimental-scenarios)
  - [Scenario 1: Baseline Workload](#scenario-1-baseline-workload)
  - [Scenario 2: Sustained Load](#scenario-2-sustained-load)
  - [Scenario 3: Burst Load](#scenario-3-burst-load)
- [Performance Metrics](#performance-metrics)
- [Data Collection](#data-collection)
- [Experimental Procedure](#experimental-procedure)
- [Reproducibility](#reproducibility)
- [Threats to Validity](#threats-to-validity)
- [Summary](#summary)
- [Related Documentation](#related-documentation)

---

# Objectives

The experimental evaluation was designed to assess whether the implemented platform satisfies the functional and non-functional requirements established during system design.

The methodology focuses on characterizing the behavior of the platform under progressively increasing workloads by evaluating latency, throughput, data integrity, scalability, and operational characteristics.

Specifically, the evaluation aims to:

* Validate near real-time processing under representative biomedical workloads.
* Evaluate the scalability of the serverless architecture as event rates increase.
* Measure end-to-end pipeline latency.
* Verify data integrity throughout the processing pipeline.
* Identify potential bottlenecks under increasing load.
* Assess the operational behavior of the managed AWS services involved in the platform.

The experiments were designed to produce quantitative and reproducible measurements that support the engineering evaluation of the proposed architecture.

---

# Research Questions

Although the report does not explicitly formulate research questions, the experimental objectives directly imply the following evaluation goals.

| Research Question                                                              | Purpose                                                        |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------- |
| Can the platform process biomedical data within the defined latency objective? | Evaluate near real-time performance.                           |
| How does the architecture behave as workload increases?                        | Assess scalability under progressively higher event rates.     |
| Does the platform preserve data integrity during processing?                   | Verify that generated events are successfully persisted.       |
| Which architectural component limits performance under high load?              | Identify system bottlenecks through quantitative measurements. |
| How do managed AWS services behave under different operating conditions?       | Characterize operational scalability and resource utilization. |

These questions guided the design of the experimental scenarios and the selection of the evaluation metrics.

---

# Experimental Environment

The evaluation was performed using a complete deployment of the platform on Amazon Web Services.

The experimental environment combined a synthetic biomedical data generator with a fully deployed serverless processing pipeline and native AWS monitoring services.

The platform was provisioned using Infrastructure as Code to ensure a consistent deployment across all experimental runs.

## Infrastructure Components

| Category            | Components                   |
| ------------------- | ---------------------------- |
| Event generation    | Biomedical Data Simulator    |
| Streaming ingestion | Amazon Kinesis Data Streams  |
| Processing          | AWS Lambda                   |
| Persistent storage  | Amazon DynamoDB, Amazon S3   |
| Error handling      | Amazon SQS Dead-Letter Queue |
| Monitoring          | Amazon CloudWatch            |
| Auditing            | AWS CloudTrail               |
| Cost monitoring     | AWS Budgets                  |
| Deployment          | Terraform                    |

## Input Dataset

The simulator reproduces biomedical signals using the **WESAD (Wearable Stress and Affect Detection)** dataset as the reference for sensor characteristics.

Rather than replaying recorded sessions directly, the simulator generates synthetic event streams that preserve the sampling frequencies and heterogeneous behavior of the wearable sensors described in the dataset.

The simulated devices include:

| Device      | Sensors                        |
| ----------- | ------------------------------ |
| RespiBAN    | ECG, EDA, EMG, TEMP, RESP, ACC |
| Empatica E4 | BVP, ACC, EDA, TEMP            |

These devices provide sampling frequencies ranging from **4 Hz** to **700 Hz**, allowing the experiments to evaluate workloads with highly heterogeneous event generation rates.

---

# Infrastructure Configuration

The experimental infrastructure remained constant throughout the evaluation to ensure comparability between scenarios.

Only the workload characteristics varied across experiments.

The report explicitly identifies the following configuration elements.

| Component                    | Experimental Configuration                                                                               |
| ---------------------------- | -------------------------------------------------------------------------------------------------------- |
| Amazon Kinesis Data Streams  | Configured with **5 shards**.                                                                            |
| AWS Lambda                   | Connected to Kinesis through Event Source Mapping using a configured `parallelization_factor` of **10**. |
| Amazon DynamoDB              | Used as the operational datastore for processed events.                                                  |
| Amazon S3                    | Used for raw event storage.                                                                              |
| Amazon SQS Dead-Letter Queue | Configured to receive events that cannot be processed successfully.                                      |
| Amazon CloudWatch            | Collected operational metrics throughout every experiment.                                               |
| Terraform                    | Provisioned the complete cloud infrastructure.                                                           |

To improve experimental consistency:

* The same infrastructure configuration was maintained across all scenarios.
* Infrastructure parameters were not modified between repetitions.
* Observed behavioral differences are therefore attributable to workload variation rather than deployment changes.

---

# Workloads

The evaluation uses synthetic biomedical workloads generated by the simulator.

Workloads are derived from the sampling frequencies of the WESAD reference dataset, preserving the heterogeneous characteristics of wearable biomedical devices.

The simulator continuously generates timestamped biomedical events that are transmitted to the streaming ingestion layer.

## Event Generation

Each generated event represents a physiological measurement associated with:

* a subject,
* a sensor,
* a timestamp,
* and the corresponding physiological value.

The simulator emits events according to the sampling frequency of each sensor, producing realistic mixtures of high-frequency and low-frequency biomedical signals.

## Load Characteristics

The evaluation varies only the workload while keeping the infrastructure configuration unchanged.

Three workload levels are defined to characterize system behavior under progressively increasing demand.

| Characteristic  | Description                                          |
| --------------- | ---------------------------------------------------- |
| Input source    | Biomedical Data Simulator                            |
| Event type      | Synthetic biomedical measurements                    |
| Sampling model  | Based on WESAD sensor frequencies                    |
| Frequency range | 4 Hz to 700 Hz                                       |
| Load variation  | Controlled through predefined experimental scenarios |

The resulting workloads include heterogeneous event streams generated by sensors operating at substantially different sampling frequencies.

This workload diversity is an essential characteristic of the evaluation because it reproduces the non-uniform event distribution expected in wearable biomedical monitoring environments.

# Experimental Scenarios

The evaluation consists of three controlled experimental scenarios designed to characterize the platform under progressively increasing workloads.

Each scenario maintains the same infrastructure configuration while modifying only the workload characteristics. This approach isolates the effect of increasing event rates on the overall system behavior.

---

## Scenario 1: Baseline Workload

### Objective

Evaluate the platform under normal operating conditions and establish a reference for subsequent experiments.

### Workload Description

The Biomedical Data Simulator generates events according to the nominal sampling frequencies defined for the simulated wearable devices.

This scenario represents the expected operating conditions of the platform without artificial load amplification.

### Expected Behaviour

The evaluation expects the platform to:

* Process incoming events continuously.
* Maintain near real-time processing.
* Preserve data integrity throughout the pipeline.
* Operate without resource saturation.

### Evaluation Criteria

The following aspects are evaluated:

* End-to-end latency.
* Event throughput.
* Processing duration.
* Data integrity.
* Lambda execution behaviour.
* Kinesis stream behaviour.
* Operational cost.

---

## Scenario 2: Sustained Load

### Objective

Evaluate the scalability of the platform under a continuously increased workload.

### Workload Description

The simulator generates a workload approximately **26 times larger** than the baseline scenario while maintaining a constant event generation rate throughout the experiment.

The workload is sustained for the complete execution period in order to evaluate steady-state behaviour.

### Expected Behaviour

The platform is expected to:

* Scale automatically without infrastructure modifications.
* Continue processing events continuously.
* Maintain stable operational behaviour under sustained demand.
* Preserve data integrity.

### Evaluation Criteria

The experiment evaluates:

* Automatic service scaling.
* End-to-end latency.
* Throughput.
* Processing duration.
* Resource utilization.
* Data integrity.
* Operational cost.

---

## Scenario 3: Burst Load

### Objective

Evaluate the platform under a short-duration workload significantly higher than the sustained operating level.

### Workload Description

The simulator generates a burst of events intended to stress the ingestion layer and evaluate the behaviour of the architecture when the incoming event rate temporarily exceeds the processing capacity of the deployed infrastructure.

Unlike the sustained workload, the burst is intentionally transient, allowing observation of the system both during overload and while recovering after the burst has ended.

### Expected Behaviour

The evaluation aims to observe:

* Behaviour of the ingestion pipeline under temporary overload.
* Automatic scaling of managed services.
* Buffering of incoming events.
* Recovery once normal workload resumes.

No assumptions are made regarding compliance with the defined Service Level Objectives during this stress scenario.

### Evaluation Criteria

The following characteristics are measured:

* End-to-end latency.
* Throughput.
* Processing duration.
* Event buffering.
* Data integrity.
* Resource utilization.
* Recovery behaviour after overload.

---

# Performance Metrics

The evaluation collects quantitative metrics describing latency, throughput, scalability, reliability, and operational cost.

These metrics are obtained from the platform itself and from AWS monitoring services.

| Metric                       | Purpose                                                                       | Unit                  |
| ---------------------------- | ----------------------------------------------------------------------------- | --------------------- |
| End-to-end latency           | Measure the elapsed time between event generation and successful persistence. | Seconds               |
| Throughput                   | Measure the number of events processed over time.                             | Events per second     |
| Lambda execution duration    | Characterize processing time within the compute layer.                        | Milliseconds          |
| Lambda concurrent executions | Evaluate automatic scaling behaviour.                                         | Concurrent executions |
| IteratorAge                  | Measure the delay between event arrival in Kinesis and Lambda processing.     | Seconds               |
| Error rate                   | Detect processing failures during execution.                                  | Percentage            |
| Data integrity               | Compare generated events with successfully persisted events.                  | Percentage            |
| Operational cost             | Estimate the economic cost of executing each experimental scenario.           | USD                   |

The report combines these metrics to evaluate both application-level behaviour and infrastructure performance.

---

# Service Level Objectives (SLOs)

The experimental evaluation defines quantitative Service Level Objectives to assess whether the platform satisfies its intended operational requirements.

| SLO                | Threshold               | Purpose                                           |
| ------------------ | ----------------------- | ------------------------------------------------- |
| End-to-end latency | **P95 < 10 seconds**    | Validate near real-time processing.               |
| Data integrity     | **Event loss below 1%** | Verify reliable event processing and persistence. |

These objectives serve as evaluation criteria throughout the experimental methodology.

The experiments measure each metric consistently across all scenarios, enabling direct comparison between workloads while using an identical infrastructure configuration.


# Data Collection

Experimental data was collected throughout every execution using the monitoring capabilities integrated into the deployed AWS infrastructure.

The methodology combines platform-generated information with operational metrics collected from managed AWS services to characterize application behavior and infrastructure performance.

## Data Sources

| Source                      | Collected Information                                                          |
| --------------------------- | ------------------------------------------------------------------------------ |
| Biomedical Data Simulator   | Generated events and timestamps used to verify data integrity.                 |
| AWS Lambda                  | Execution duration, concurrent executions, invocation count and error metrics. |
| Amazon Kinesis Data Streams | IteratorAge and stream processing metrics.                                     |
| Amazon CloudWatch           | Operational metrics collected during every experimental scenario.              |
| AWS Budgets                 | Estimated operational cost of the experimental executions.                     |

The report compares the number of generated events with the number of successfully processed events to evaluate data integrity throughout the pipeline.

No additional instrumentation beyond the deployed platform is described.

---

# Experimental Procedure

Each experimental scenario follows the same execution workflow.

Maintaining an identical procedure ensures that differences between scenarios are attributable to workload variation rather than changes to the experimental process.

## Execution Workflow

1. Deploy the cloud infrastructure using Terraform.
2. Verify that all required AWS services are operational.
3. Configure the Biomedical Data Simulator for the selected experimental scenario.
4. Start the monitoring infrastructure.
5. Execute the workload for the selected scenario.
6. Collect operational metrics throughout the execution.
7. Record the generated and successfully processed event counts.
8. Estimate the operational cost associated with the execution.
9. Repeat the procedure for each experimental scenario using the same infrastructure configuration.

Throughout the evaluation:

* Infrastructure parameters remain unchanged.
* Monitoring is active during the complete execution.
* Workloads are the only variable modified between scenarios.

This controlled methodology enables direct comparison across all evaluated workloads.

---

# Reproducibility

The experimental methodology incorporates several elements that improve reproducibility.

| Element                            | Contribution                                                                                         |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Infrastructure as Code             | Terraform enables consistent deployment of the complete AWS infrastructure.                          |
| Managed AWS services               | The evaluation uses managed cloud services with predefined configurations.                           |
| Fixed infrastructure configuration | Infrastructure parameters remain constant across all scenarios.                                      |
| Controlled workloads               | Experimental scenarios are executed using predefined workload configurations.                        |
| Automated event generation         | The Biomedical Data Simulator produces repeatable workloads based on the WESAD sampling frequencies. |
| Standardized monitoring            | Metrics are collected consistently using Amazon CloudWatch throughout every experiment.              |

Keeping the deployment and execution procedure constant allows the experiments to be repeated under equivalent conditions while isolating workload intensity as the independent variable.

---

# Threats to Validity

The report identifies several factors that should be considered when interpreting the experimental methodology.

## Infrastructure Scope

The evaluation was conducted using a single AWS deployment configuration.

Alternative infrastructure configurations, such as different Kinesis shard allocations or multi-region deployments, were not evaluated.

## Workload Scope

The experiments use synthetic workloads generated from the sampling frequencies of the WESAD dataset.

Although these workloads reproduce heterogeneous biomedical sensor behavior, they may not capture every characteristic of production clinical environments.

## Prototype Scope

The evaluated platform represents a prototype intended to validate the proposed architecture.

Several extensions identified as future work, including automatic replay of failed events and multi-region deployment, are outside the scope of the experimental methodology.

---

# Summary

The evaluation methodology provides a controlled and reproducible framework for assessing the operational characteristics of the platform.

A constant infrastructure configuration, standardized monitoring, and predefined workload scenarios enable direct comparison across experiments while isolating workload intensity as the primary experimental variable.

The methodology combines quantitative performance metrics, operational monitoring, and data integrity verification to characterize the behavior of the platform under progressively increasing workloads without modifying the underlying cloud infrastructure.

---

# Related Documentation

* `architecture.md`
* `engineering-decisions.md`
* `evaluation.md`
* `results.md`
* `cost-analysis.md`
