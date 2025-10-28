### **Experiment 2: Robustness and Order Sensitivity**

This experiment investigates two critical aspects of LQO reliability: their sensitivity to the training data's structure and order, and their stability when executing the same query multiple times.

#### **2.1 Sensitivity to Train/Test Splits and Workload Orderings**

This sub-experiment systematically evaluates how LQO performance is affected by different strategies for partitioning and ordering the training data.

**Methodology:**
1.  **Workload:** The experiment is conducted on the Join Order Benchmark (JOB).
2.  **Procedure:**
    *   Each LQO is trained and evaluated independently on every combination of the following split and ordering strategies:
        *   **Train/Test Split Strategies:**
            1.  **Leave-One-Out Sampling:** Hold out one query instance per template for testing.
            2.  **Random Sampling:** A standard 80/20 random split of all queries.
            3.  **Template-Based Splitting:** Hold out entire query templates for testing.
        *   **Training Workload Orderings:**
            1.  **Random Order:** Training queries are presented in a shuffled, random sequence.
            2.  **Ascending Complexity:** Training queries are ordered by the number of joins, from simple to complex.
            3.  **Ascending Latency:** Training queries are ordered by their execution time on the PostgreSQL baseline, from fastest to slowest.
3.  **Metrics:**
    *   The primary metric is the overall workload latency, compared across all 9 training combinations to identify which strategies yield the most effective and stable models.

#### **2.2 Workload Robustness and Execution Stability**

This sub-experiment measures how consistently LQOs produce plans of the same quality across multiple executions of the same query.

**Methodology:**
1.  **Procedure:**
    *   During the evaluation phase of Section 2.1, each query in the test set is executed **three times consecutively**.
    *   The execution times for these three runs are recorded.
2.  **Metrics:**
    *   **Execution Time Variability:** The range and standard deviation of latencies for the same query across consecutive runs. A robust optimizer should exhibit low variability.
    *   **Plan Consistency Analysis:** The generated query plans are inspected to determine if the LQO produces the same plan on each run or if the plan itself changes, leading to performance fluctuations.