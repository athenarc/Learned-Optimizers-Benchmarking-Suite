### **Experiment 1: Accumulative Performance and Value Model Accuracy**

This foundational experiment evaluates the end-to-end performance of Learned Query Optimizers (LQOs) against the classic optimizer under a unified, consistent evaluation setting. The goal is to establish a fair performance baseline and investigate the relationship between an LQO's internal predictive accuracy and its real-world execution performance.

#### **1.1 End-to-End Performance Evaluation**

This sub-experiment measures and compares the total query processing time for each optimizer, breaking it down into planning (inference) and execution costs.

**Methodology:**
1.  **Workloads:** Experiments are conducted on three standard analytical benchmarks with diverse characteristics:
    *   **JOB (Join Order Benchmark):** Real-world data with complex correlations and deep joins.
    *   **TPC-DS:** Complex schema with synthetic data and algorithmic skew.
    *   **TPC-H:** Simple schema with purely synthetic data.
2.  **Procedure:**
    *   Each LQO is trained on the respective benchmark's training set for the number of epochs specified in its original paper to reach competitive performance.
    *   During evaluation, each optimizer (including the PostgreSQL baseline) executes the full test workload.
    *   For each query, both the **inference time** (planning cost) and the **execution latency** are recorded.
    *   To ensure stable measurements, the full workload execution is repeated three times, and the results are aggregated.
3.  **Metrics:**
    *   **Accumulative Inference Time:** The total time spent on query planning.
    *   **Accumulative Execution Latency:** The total time spent executing the generated plans.
    *   **Overall Speedup:** The end-to-end performance gain relative to the PostgreSQL baseline.
    *   **Performance Distribution:** Analysis of latency distribution (e.g., boxplots) to understand worst-case behavior and tail latency.

#### **1.2 Value Model Accuracy Assessment**

This sub-experiment assesses the accuracy of the internal predictive models (Value Models) used by regression-based LQOs (e.g., NEO, BAO, LOGER).

**Methodology:**
1.  **Procedure:**
    *   During the end-to-end performance evaluation (Section 1.1), for each query plan generated and executed by an LQO, two values are recorded:
        1.  The LQO's internal **predicted latency**.
        2.  The **actual execution latency**.
2.  **Metrics:**
    *   **Prediction Q-Error:** The relative difference between the predicted and actual latencies. This metric quantifies the accuracy of the value model. A distribution of Q-Errors is analyzed for each optimizer to understand its median accuracy and the presence of severe mispredictions (outliers).
