import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np
import os

# Always run from the project root folder
os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

# CONFIG
DATA_DIR   = "analysis/query_results"   # folder where your CSV files are
OUTPUT_DIR = "analysis/charts"          # folder where charts will be saved

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Color
PRIMARY   = "#2563EB"
SECONDARY = "#10B981"
ACCENT    = "#F59E0B"
DANGER    = "#EF4444"
PURPLE    = "#8B5CF6"
COLORS    = [PRIMARY, SECONDARY, ACCENT, DANGER, PURPLE, "#06B6D4", "#EC4899", "#84CC16"]

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "white",
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "grid.linestyle":   "--",
    "font.family":      "sans-serif",
    "axes.spines.top":  False,
    "axes.spines.right":False,
})

def save(name):
    path = os.path.join(OUTPUT_DIR, name)
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Saved: {name}")

# 1. MONTHLY REVENUE 
print("Building charts...")
df = pd.read_csv(f"{DATA_DIR}/monthly_revenue.csv")
df["month"] = pd.to_datetime(df["month"])
df["total_revenue"] = df["total_revenue"].astype(float)
df["total_orders"]  = df["total_orders"].astype(int)

fig, ax1 = plt.subplots(figsize=(14, 5))
ax1.fill_between(df["month"], df["total_revenue"], alpha=0.15, color=PRIMARY)
ax1.plot(df["month"], df["total_revenue"], color=PRIMARY, linewidth=2.5, marker="o", markersize=5)
ax1.set_ylabel("Revenue (R$)", color=PRIMARY, fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1e6:.1f}M"))
ax2 = ax1.twinx()
ax2.bar(df["month"], df["total_orders"], width=20, alpha=0.3, color=SECONDARY, label="Orders")
ax2.set_ylabel("Orders", color=SECONDARY, fontsize=11)
ax2.spines["top"].set_visible(False)
ax1.set_title("Monthly Revenue & Orders — Jan 2017 to Aug 2018", fontsize=14, fontweight="bold", pad=15)
ax1.set_xlabel("")
fig.tight_layout()
save("01_monthly_revenue.png")

# 2. PEAK HOURS HEATMAP
df = pd.read_csv(f"{DATA_DIR}/peak_days_hours.csv")
df["total_orders"] = df["total_orders"].astype(int)
day_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
pivot = df.pivot_table(index="day_name", columns="hour", values="total_orders", aggfunc="sum")
pivot = pivot.reindex(day_order)

fig, ax = plt.subplots(figsize=(16, 5))
sns.heatmap(pivot, cmap="Blues", ax=ax, linewidths=0.3,
            cbar_kws={"label": "Orders", "shrink": 0.8},
            fmt=".0f", annot=False)
ax.set_title("Purchase Heatmap — Day of Week vs Hour of Day", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Hour of Day", fontsize=11)
ax.set_ylabel("")
fig.tight_layout()
save("02_peak_heatmap.png")

# 3. PAYMENT METHODS 
df = pd.read_csv(f"{DATA_DIR}/payment_methods.csv")
df["pct_share"] = df["pct_share"].astype(float)
labels = [t.replace("_", " ").title() for t in df["payment_type"]]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

# Pie chart
wedges, texts, autotexts = ax1.pie(
    df["pct_share"], labels=labels, autopct="%1.1f%%",
    colors=COLORS[:len(df)], startangle=90,
    wedgeprops={"edgecolor": "white", "linewidth": 2}
)
for t in autotexts:
    t.set_fontsize(10)
ax1.set_title("Payment Method Share", fontsize=13, fontweight="bold")

# Avg installments bar
ax2.bar(labels, df["avg_installments"], color=COLORS[:len(df)], edgecolor="white", linewidth=1.5)
ax2.set_title("Average Installments by Payment Type", fontsize=13, fontweight="bold")
ax2.set_ylabel("Avg Installments")
ax2.set_ylim(0, df["avg_installments"].max() * 1.3)
for i, v in enumerate(df["avg_installments"]):
    ax2.text(i, v + 0.05, f"{v:.1f}", ha="center", fontsize=10, fontweight="bold")

fig.tight_layout()
save("03_payment_methods.png")

# 4. CUSTOMER SEGMENTS 
df = pd.read_csv(f"{DATA_DIR}/customer_segments.csv")

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.barh(df["segment"], df["customers"], color=[PRIMARY, SECONDARY, ACCENT][:len(df)],
               edgecolor="white", height=0.5)
ax.set_title("Customer Segmentation", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Number of Customers")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
for bar, (_, row) in zip(bars, df.iterrows()):
    ax.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2,
            f"{row['customers']:,} ({row['pct']}%)", va="center", fontsize=11, fontweight="bold")
ax.set_xlim(0, df["customers"].max() * 1.25)
fig.tight_layout()
save("04_customer_segments.png")

# 5. SPEND DISTRIBUTION
df = pd.read_csv(f"{DATA_DIR}/spend_distribution.csv")
percentiles = ["p25", "median", "p75", "p90", "mean"]
labels      = ["25th\nPercentile", "Median\n50th", "75th\nPercentile", "90th\nPercentile", "Mean\nAverage"]
values      = [float(df[p].iloc[0]) for p in percentiles]
bar_colors  = [SECONDARY, PRIMARY, ACCENT, DANGER, PURPLE]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(labels, values, color=bar_colors, edgecolor="white", linewidth=1.5, width=0.6)
for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
            f"R${val:.0f}", ha="center", fontsize=11, fontweight="bold")
ax.set_title("Customer Spend Distribution", fontsize=14, fontweight="bold", pad=15)
ax.set_ylabel("Amount (R$)")
ax.set_ylim(0, max(values) * 1.2)
fig.tight_layout()
save("05_spend_distribution.png")

# 6. TOP CATEGORIES REVENUE 
df = pd.read_csv(f"{DATA_DIR}/categories_revenue.csv")
df["total_revenue"] = df["total_revenue"].astype(float)
df["category"] = df["category"].str.replace("_", " ").str.title()
df = df.sort_values("total_revenue", ascending=True)

fig, ax = plt.subplots(figsize=(12, 8))
colors = [PRIMARY if i >= len(df)-3 else "#93C5FD" for i in range(len(df))]
bars = ax.barh(df["category"], df["total_revenue"], color=colors, edgecolor="white", height=0.7)
ax.set_title("Top 15 Product Categories by Revenue", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Total Revenue (R$)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1e6:.1f}M"))
for bar, val in zip(bars, df["total_revenue"]):
    ax.text(bar.get_width() + 5000, bar.get_y() + bar.get_height()/2,
            f"R${val/1e6:.2f}M", va="center", fontsize=9)
fig.tight_layout()
save("06_categories_revenue.png")

# 7. LATE DELIVERY BY STATE
df = pd.read_csv(f"{DATA_DIR}/late_by_state.csv")
df["pct_late"] = df["pct_late"].astype(float)
df = df.sort_values("pct_late", ascending=True)
df["customer_state"] = df["customer_state"].str.upper()

fig, ax = plt.subplots(figsize=(10, 9))
colors = [DANGER if v > 15 else ACCENT if v > 10 else SECONDARY for v in df["pct_late"]]
bars = ax.barh(df["customer_state"], df["pct_late"], color=colors, edgecolor="white", height=0.7)
ax.axvline(df["pct_late"].mean(), color=PRIMARY, linestyle="--", linewidth=1.5,
           label=f"Average: {df['pct_late'].mean():.1f}%")
ax.set_title("Late Delivery Rate by State (%)", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("% Late Deliveries")
for bar, val in zip(bars, df["pct_late"]):
    ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
            f"{val:.1f}%", va="center", fontsize=9, fontweight="bold")
ax.legend(fontsize=10)
fig.tight_layout()
save("07_late_by_state.png")

# 8. DELIVERY STATS KPI
df = pd.read_csv(f"{DATA_DIR}/delivery_stats.csv")
stats = {
    "Total Orders":       (int(df["total_orders"].iloc[0]), PRIMARY),
    "Late Orders":        (int(df["late_orders"].iloc[0]), DANGER),
    "Late Rate":          (f"{float(df['pct_late'].iloc[0])}%", ACCENT),
    "Avg Actual Days":    (f"{float(df['avg_actual_days'].iloc[0])} days", SECONDARY),
    "Avg Estimated Days": (f"{float(df['avg_estimated_days'].iloc[0])} days", PURPLE),
}

fig, axes = plt.subplots(1, 5, figsize=(16, 3))
for ax, (label, (value, color)) in zip(axes, stats.items()):
    ax.set_facecolor(color + "18")
    ax.text(0.5, 0.6, f"{value:,}" if isinstance(value, int) else value,
            ha="center", va="center", fontsize=20, fontweight="bold", color=color,
            transform=ax.transAxes)
    ax.text(0.5, 0.2, label, ha="center", va="center", fontsize=10, color="#374151",
            transform=ax.transAxes)
    for spine in ax.spines.values():
        spine.set_edgecolor(color)
        spine.set_linewidth(2)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.grid(False)

fig.suptitle("Delivery Performance Overview", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
save("08_delivery_stats.png")

# 9. REVENUE BY STATE
df = pd.read_csv(f"{DATA_DIR}/geo_revenue.csv")
df["total_revenue"] = df["total_revenue"].astype(float)
df["customer_state"] = df["customer_state"].str.upper()
top = df.nlargest(15, "total_revenue")

fig, ax = plt.subplots(figsize=(12, 6))
bar_colors = [PRIMARY if i == 0 else SECONDARY if i == 1 else "#93C5FD" for i in range(len(top))]
ax.bar(top["customer_state"], top["total_revenue"], color=bar_colors, edgecolor="white")
ax.set_title("Top 15 States by Revenue", fontsize=14, fontweight="bold", pad=15)
ax.set_ylabel("Total Revenue (R$)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1e6:.1f}M"))
for i, (_, row) in enumerate(top.iterrows()):
    ax.text(i, row["total_revenue"] + 50000, f"R${row['total_revenue']/1e6:.1f}M",
            ha="center", fontsize=8, fontweight="bold")
fig.tight_layout()
save("09_geo_revenue.png")

# 10. LATE RATE VS REVIEW SCORE
df = pd.read_csv(f"{DATA_DIR}/late_vs_score.csv")
df["avg_review_score"] = df["avg_review_score"].astype(float)

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(df["late_rate_bucket"], df["avg_review_score"],
              color=[SECONDARY, ACCENT, ACCENT, DANGER, DANGER][:len(df)],
              edgecolor="white", width=0.6)
ax.set_ylim(3.5, df["avg_review_score"].max() * 1.05)
ax.set_title("Impact of Late Deliveries on Review Score", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Late Delivery Rate Bucket")
ax.set_ylabel("Average Review Score")
for bar, val in zip(bars, df["avg_review_score"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
            f"⭐ {val:.3f}", ha="center", fontsize=11, fontweight="bold")
fig.tight_layout()
save("10_late_vs_score.png")

# 11. PRICE TIERS 
df = pd.read_csv(f"{DATA_DIR}/price_tiers.csv")
df["price_tier"] = df["price_tier"].str.replace(r"^\d_", "", regex=True)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
ax1.bar(df["price_tier"], df["total_orders"], color=COLORS[:len(df)], edgecolor="white", width=0.6)
ax1.set_title("Orders by Price Tier", fontsize=13, fontweight="bold")
ax1.set_ylabel("Total Orders")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
for i, v in enumerate(df["total_orders"]):
    ax1.text(i, v + 300, f"{v:,}", ha="center", fontsize=10, fontweight="bold")

ax2.bar(df["price_tier"], df["avg_score"], color=COLORS[:len(df)], edgecolor="white", width=0.6)
ax2.set_title("Avg Review Score by Price Tier", fontsize=13, fontweight="bold")
ax2.set_ylabel("Avg Review Score")
ax2.set_ylim(3.5, df["avg_score"].max() * 1.08)
for i, v in enumerate(df["avg_score"]):
    ax2.text(i, v + 0.005, f"⭐ {v:.2f}", ha="center", fontsize=10, fontweight="bold")

fig.tight_layout()
save("11_price_tiers.png")

# 12. TOP SELLERS
df = pd.read_csv(f"{DATA_DIR}/top_sellers.csv")
df["total_revenue"] = df["total_revenue"].astype(float)
df["seller_label"]  = df["seller_id"].str[:8] + "... (" + df["seller_state"].str.upper() + ")"
top10 = df.head(10).sort_values("total_revenue", ascending=True)

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(top10["seller_label"], top10["total_revenue"],
               color=PRIMARY, edgecolor="white", height=0.6)
ax.set_title("Top 10 Sellers by Revenue", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Total Revenue (R$)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1e3:.0f}K"))
for bar, (_, row) in zip(bars, top10.iterrows()):
    ax.text(bar.get_width() + 1000, bar.get_y() + bar.get_height()/2,
            f"R${row['total_revenue']/1e3:.0f}K  |  ⭐{row['avg_review_score']}",
            va="center", fontsize=9)
ax.set_xlim(0, top10["total_revenue"].max() * 1.3)
fig.tight_layout()
save("12_top_sellers.png")

# 13. CROSS STATE FLOW
df = pd.read_csv(f"{DATA_DIR}/cross_state.csv")
df["seller_state"]   = df["seller_state"].str.upper()
df["customer_state"] = df["customer_state"].str.upper()
top15 = df.head(15)
labels = top15["seller_state"] + " → " + top15["customer_state"]

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(labels, top15["total_orders"], color=COLORS * 3, edgecolor="white", height=0.7)
ax.set_title("Top 15 Seller → Customer State Flows", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Total Orders")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
for bar, val in zip(bars, top15["total_orders"]):
    ax.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2,
            f"{val:,}", va="center", fontsize=9, fontweight="bold")
fig.tight_layout()
save("13_cross_state.png")

print("\nAll charts saved to:", OUTPUT_DIR)