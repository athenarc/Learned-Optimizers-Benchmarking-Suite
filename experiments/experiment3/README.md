This experiment analyzes the learning process of each LQO to understand its convergence speed, stability, and the effectiveness of different training paradigms. The goal is to move beyond evaluating only the final model and instead gain insights into the training procedure itself.

#### **3.1 Learning Trajectory Under Different Training Policies**

This sub-experiment tracks optimizer performance at various checkpoints during training, guided by different definitions of "progress."

**Methodology:**
1.  **Workload:** The experiment is conducted on the Join Order Benchmark (JOB), using the "Random Split - Ascending Complexity" setup from Experiment 2.
2.  **Procedure:**
    *   During a single, extended training session for each LQO, model snapshots (checkpoints) are saved according to three distinct policies:
        1.  **Epoch-Based:** A snapshot is saved every 10 full passes (epochs) over the training data.
        2.  **Loss-Based:** A snapshot is saved each time the model's internal validation loss improves beyond a certain threshold, tracking model convergence.
        3.  **Query-Exposure-Based:** A snapshot is saved after the optimizer has processed a fixed number of training queries (e.g., every 250 queries), tracking experience gathered.
    *   For every saved checkpoint, the corresponding model's performance is evaluated on the held-out test set.
3.  **Metrics:**
    *   **Learning Trajectory Plots:** Performance (e.g., total workload latency) is plotted against the metric of each policy (epochs, loss, or queries seen). This visualizes how quickly each LQO learns, whether its performance is stable, and which training policy provides the most consistent learning signal.
    *   **Internal Metric Correlation:** For regression-based LQOs, internal metrics like Q-Error are also tracked to see how they correlate with end-to-end performance improvements during training.