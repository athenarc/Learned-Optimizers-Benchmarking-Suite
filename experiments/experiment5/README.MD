# Experiment 5: Evaluating Generalization and Adaptivity

This experiment evaluates how well learned query optimizers generalize to new queries, unfamiliar database schemas, and shifts in data distributions.

## 5.1 Generalization to Unseen Queries

We assess the optimizers’ ability to handle queries and scenarios not encountered during training.

### Dimension 1: Distribution Generalization

Evaluate performance on entirely different workload distributions between training and testing workloads.

In order to calculate the distribution of a workload we identify for each query:
- Tables used: the names of the database tables referenced.
- Joins performed: The joins that are present in each query
- Predicates applied: Filters used in the query
- (More to be added)

The aggregated values of these features across all queries are used to construct empirical categorical distributions per feature type. This forms the **workload distribution**

**Workload distance** 

To quantify the similarity or divergence between two workloads, we compute the Jensen-Shannon Divergence (JS) between their corresponding feature distributions:
- We align the distributions by unifying their categorical bins.
- We compute the JS Distance per distribution, between the aligned distributions.
- We average the distances across all distributions to obtain the overall workload distance.

**Procedure:**

1. Train on the standard JOB workload.
2. Test on the JOB-Extended workload, which according to our calculations exhibits a distance of 0.8 from  the original JOB workload.

### Dimension 2: Query Complexity Generalization

Test optimizer performance as query complexity increases.

**Procedure:**

1. Train on JOB queries with fewer than 10 joins.
2. Test on JOB queries with more than 10 joins.

### Dimension 3: Selectivity Generalization

Measure performance when selectivity distributions change across queries.

**Procedure:**

1. Generate variants of JOB queries with varying predicate selectivities:

   * \~0% (extremely low)
   * \~10% (very low)
   * \~20% (low)
   * \~60% (medium)
   * \~80% (high)
   * \~100% (extremely high)

   *(Numerical: use `<`, `>` with sampled values. Categorical: sample by frequency from least to most common.)*
2. Train optimizers on low-selectivity variants.
3. Test optimizers on high-selectivity variants.

### Dimension 4: Database Coverage Generalization

Analyze how optimizers adapt to broader database coverage by the workload.

**Procedure:**

1. Define or estimate a database coverage metric.
2. Train on queries with lower coverage.
3. Test on queries with higher coverage.

---

## 5.2 Adaptability to New Schemas

This section evaluates how well the optimizers adapt to changes in database schema—both within a workload and across schema designs.

### Dimension 1: Workload-Specific Schema Shift

Assess optimizer performance when a new table appears in test-time queries that was absent during training.

**Procedure:**

1. Train on JOB queries that do **not** include the `char_name` table.
2. Test on queries that include lookups on the `char_name` table.

### Dimension 2: Structural Schema Change

Evaluate generalization when the underlying schema structure changes.

**Procedure:**

1. Train on the Star Schema Benchmark (SSB) using its standard schema and workload.
2. Modify the schema by splitting the SSB fact table into two tables, mimicking the TPC-H design.
3. Test with conceptually equivalent queries that now involve joins across the split tables.

--- 

## 5.3 Adaptation to Distribution Shifts

Evaluate the optimizer's ability to adapt to distribution shifts to the underlying data

**Procedure:**

 1. Split the Stack dataset (2008–2019) into 3-year intervals
 2. Use half of the intervals to train the optimizers and the other half to test them.
