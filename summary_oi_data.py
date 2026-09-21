import pandas as pd

# 1. Load the parquet file
df = pd.read_parquet(r"C:\Users\narasimharao.allaka\Downloads\part-0 (2).parquet")


# 2. Convert 'fetched_at' to datetime and shift to IST (UTC +5:30)
df["fetched_at"] = pd.to_datetime(df["fetched_at"])
df["fetched_at_ist"] = df["fetched_at"] + pd.Timedelta(hours=5, minutes=30)

# 3. Sort chronologically by the new IST time index
df = df.sort_values("fetched_at_ist")
df.to_csv(r"C:\Users\narasimharao.allaka\Downloads\part-0 (2).csv")

# 4. Calculate group counts (how many rows share the exact same IST timestamp)
df["grp_count"] = df.groupby("fetched_at_ist")["fetched_at_ist"].transform("count")

# 5. Group by the IST time index to aggregate metrics and keep group counts
grouped = df.groupby("fetched_at_ist").agg({
    **{col: "mean" for col in df.select_dtypes(include="number").columns if col != "grp_count"},
    "grp_count": "first"
}).reset_index()

# 6. Calculate grp_diff (the time difference between consecutive fetched_at_ist timestamps)
grouped["grp_diff"] = grouped["fetched_at_ist"].diff()

# 7. Compute the numerical differences (dff) between consecutive time indexes
numeric_cols = grouped.select_dtypes(include="number").columns
numeric_cols = [c for c in numeric_cols if c != "grp_count"] # exclude count from diffing

numeric_diffs = grouped[numeric_cols].diff()
numeric_diffs = numeric_diffs.rename(columns=lambda x: f"{x}_dff")

# 8. Combine everything safely using concat side-by-side (resetting indices)
final_df = pd.concat([grouped, numeric_diffs], axis=1)

# Display a preview of the required columns
print("--- Final Dataset Preview ---")
print(final_df[["fetched_at_ist", "grp_count", "grp_diff"]].head())

# Optional: Save the output back to a new parquet or CSV file
final_df.to_csv(r"C:\Users\narasimharao.allaka\Downloads\part-0 (2)-dff.csv")