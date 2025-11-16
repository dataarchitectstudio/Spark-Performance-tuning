# 🚀 How I Reduced Spark Job Runtime by 5X & Saved Big Cost for My Pipeline

## 📌 Background Story
At my company, we process around **500 million rows** with **1,200+ columns** every night.  
We also add **250+ columns** during ETL.  
The Spark job was taking **3 hours** and consuming heavy compute cost.

After analysing the Spark logical plan, I found that the pipeline was using **250+ `withColumn()`** calls.  
This created hundreds of **Project Nodes**, making the optimizer extremely slow.

---

# 🔍 Why `withColumn()` Was Slow
Every `withColumn()` triggers:
- Analyzer again
- Optimizer again
- 1 new **Project Node**
- Larger Logical Plan

---

# 🧠 What Are Project Nodes?
Project Node means:  
**"These are the columns Spark should output next."**

More Project Nodes → slower job startup.

---

# 🆚 Explain Plan Comparison

## ❌ Using `withColumn()` 250 times
```
Project
  Project
    Project
      ...
        Project
          Scan parquet
```

## 🔄 Using `selectExpr()`
```
Project
  Scan parquet
```

## ✅ Using `withColumns()`
```
Project
  Scan parquet
```

---

# 🛠️ How I Fixed It

## Create new columns
```python
new_cols = [f"feature_{i}" for i in range(250)]
dummy_map = {name: lit(None).cast("string") for name in new_cols}
```

## Optimized version
```python
df2 = df.withColumns(dummy_map)
```

---

# 🎉 Result
| Metric | Before | After |
|--------|--------|--------|
| Runtime | 180 mins | 36 mins |
| Project Nodes | 250+ | 1 |
| Monthly Cost | $4,000 | $1,200 |
| **Savings** | — | **$2,800 / month** |

---

# ❓ FAQ

✔️ Use `withColumn()` only for few columns  
✔️ For 100+ columns → use `select()` or `withColumns()`  
✔️ If jobs show high uptime but low stage time → driver is slow

---

# 🏁 Final Summary
- `withColumn()` is fine for small cases  
- Use `withColumns()`/`select()` for large column additions  
- Fewer Project Nodes = faster jobs  
- This optimization saved us **₹27.6 Lakhs per year**

