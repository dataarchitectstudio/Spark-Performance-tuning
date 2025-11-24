# 🚀 Disabling a AQE Made My Job 10× Faster  
### *A Step-by-Step Debugging Story of AQE Gone Wrong*

This README documents one of the most interesting performance issues I’ve debugged —  
where **turning off Spark’s Adaptive Query Execution (AQE) made a 5 TB job 10× faster**.

If you work with Spark, large joins, or shuffle-heavy pipelines, this story will save you hours.

---

# 🎬 Scene 1: The Strange Behavior

Everything “should” have worked.  
5 TB Parquet input → Aggregation → Heavy joins → Write.

But the job behaved unpredictably:

- ⚡ Some runs finished in **20 minutes**
- 🐌 Others took **2+ hours**
- 💥 Driver memory spikes
- 🔁 Shuffle rewrites
- 🔄 Multiple broadcast attempts

Same data.  
Same cluster.  
Same code.  

Yet completely different performance.

Time to investigate.

---

# 🔍 Scene 2: Checking Spark UI — Something Was Off

The Spark UI revealed:

- Shuffle partition counts changing each run  
- Broadcast relations emitted multiple times  
- Stages invalidated and recomputed  
- Join strategy switching mid-execution  

These signs hinted that **AQE (Adaptive Query Execution)** was changing the execution plan at runtime.

---

# 🧩 Scene 3: Digging Into Logs — Found the Culprit

Analyzing Spark event logs revealed that AQE was:

- Detecting minor skew  
- Recalculating shuffle partitions  
- Switching join strategies multiple times mid-flight  
- Invalidating completed stages  
- Retrying broadcasts  
- Triggering shuffle rewrites  

Below is a realistic production-style log sample that represents what was observed:

```log
INFO SparkContext: Running Spark version 3.3.1
INFO AdaptiveSparkPlanExec: Collecting runtime statistics for stage 38...
INFO AdaptiveSparkPlanExec: Detected skew in join keys (partitionId=17, size=1.82 GB, median=145 MB)
INFO AdaptiveSparkPlanExec: Triggering adaptive re-optimization due to skew detection
INFO AdaptiveSparkPlanExec: Updating shuffle partition count: 200 -> 260

INFO AdaptiveSparkPlanExec: Re-optimizing join strategy:
INFO AdaptiveSparkPlanExec: Switching SortMergeJoin -> BroadcastHashJoin (small table = dim_customer)

INFO BroadcastExchangeExec: Broadcasting relation (dim_customer), size: 480.3 MB
WARN BroadcastExchangeExec: Plan updated during broadcast. Retrying collection...

INFO DAGScheduler: Cancelling ShuffleMapStage 42 due to updated adaptive plan
INFO DAGScheduler: Starting ShuffleMapStage 42 (attempt 2)
INFO ShuffleExternalSorter: Spilling map output for partition 17 (1.82 GB) to disk

INFO BroadcastExchangeExec: Re-collecting broadcast for relation dim_customer (attempt 2)
INFO BroadcastExchangeExec: Broadcasting broadcast_1 with size 482.1 MB
WARN MemoryStore: broadcast_1 too large to fit in memory; spilling to disk

INFO Executor: Finished task 17.0 in stage 42.1 (TID 98123) in 15433 ms
INFO DAGScheduler: Stage 42 (attempt 2) completed in 54.4 s

INFO AdaptiveSparkPlanExec: Runtime stats changed. Re-optimizing join again...
INFO AdaptiveSparkPlanExec: Join strategy changed: BroadcastHashJoin -> SortMergeJoin

WARN BroadcastExchangeExec: Discarding broadcast_1 due to plan change
INFO DAGScheduler: Invalidating completed Stage 42. Recomputing due to new plan.

INFO DAGScheduler: Resubmitting Stage 42 (attempt 3)
INFO ShuffleExternalSorter: Writing 260 shuffle files to disk (total size: 3.8 GB)
INFO DiskBlockObjectWriter: Rewriting shuffle partition 17 (size: 1.2 GB)

WARN DriverEndpoint: Driver memory usage is 89%. GC pressure increasing.
INFO TransportClientFactory: Serialized 486 MB to driver in 7.9s

INFO DAGScheduler: Starting ResultStage 52 (attempt 1)
INFO AdaptiveSparkPlanExec: Statistics updated. Invalidating stage 52.
INFO DAGScheduler: Submitting Stage 52 (attempt 2)

INFO Executor: Running tasks in stage 52.1 (TID 98354, 98355)
INFO DAGScheduler: Stage 52 (attempt 2) completed in 41.8s

INFO JobScheduler: Job 17 finished in 124.8 minutes
```


# ❗ Scene 4: Understanding the Real Problem

AQE is extremely useful when:

- Data distribution is consistent  
- Join patterns are predictable  
- Shuffle sizes are stable  
- Runtime stats do not fluctuate much  

But AQE becomes **dangerous** when your workload has:

- Large fact tables  
- Skewed join keys  
- Bursty or uneven shuffle data  
- Custom partitioning logic  
- Slight skew that triggers unnecessary plan rewrites  
- Large broadcast tables  
- Highly variable runtime statistics  

In such cases, AQE becomes **too aggressive**, causing:

- Multiple execution plan rewrites  
- Stage invalidation and recomputation  
- Broadcast retries  
- Shuffle explosions  
- Excessive driver memory usage  
- Unpredictable, unstable run times  

In my scenario, **all these conditions happened together**, making AQE unstable and unpredictable.

---

# 🩹 Scene 5: The Fix

After confirming that AQE was repeatedly re-planning mid-execution,  
I disabled Adaptive Query Execution **only for this specific pipeline**:

```python
spark.conf.set("spark.sql.adaptive.enabled", "false")
```


AQE made the plan unstable because the runtime statistics kept fluctuating during skew-heavy joins.

---

# 🧰 Debugging Checklist (If You Suspect AQE Issues)

Here’s a quick checklist to identify if AQE is hurting your Spark workload:

### ✔️ 1. Shuffle partition count changes between identical runs  
→ AQE is recalculating partition sizes dynamically.

### ✔️ 2. Broadcast happens more than once  
→ Look for: `Retrying broadcast due to plan update`.

### ✔️ 3. Stages recompute even without failures  
→ AQE invalidates completed stages when runtime stats change.

### ✔️ 4. Join strategy switches mid-execution  
→ SortMergeJoin ↔ BroadcastHashJoin ↔ Shuffle Hash Join.

### ✔️ 5. Skew detected repeatedly  
→ `Detected skew in partition...`.

### ✔️ 6. Driver memory spikes  
→ Repeated broadcast collection stresses the driver.

### ✔️ 7. Job runtime is inconsistent  
→ Same job, same data, different execution time.

If **3 or more symptoms** appear, AQE is likely causing instability.

---

# 🧠 Key Learnings

- AQE is powerful but not always appropriate.
- Slight skew on large datasets can trigger unnecessary re-planning.
- Re-planning loops can be more expensive than the original plan.
- Predictable execution often outperforms adaptive planning in complex ETL pipelines.
- AQE should be tuned **per pipeline**, not just enabled globally.
- Logs + Spark UI provide the strongest clues when AQE becomes unstable.
- Understanding the dataset is crucial before relying on adaptive optimizations.

---

# 🔚 Final Takeaway

> **Smart optimizations aren’t always fast.  
> Sometimes the simplest, most stable execution plan performs the best.**

This debugging journey left one clear message:

**Understand your data, know your workload, and don’t blindly trust any “auto” optimization — even from Spark.**

AQE wasn’t wrong;  
It just wasn’t right for *this workload*.

---
