import time
import matplotlib.pyplot as plt

results = {
    "types": [
        "zstd_3-4K",
        "gzip_6-4K",
        "snappy-4K",
        "zstd_3-8K",
        "gzip_6-8K",
        "snappy-8K",
        "zstd_3-32K",
        "gzip_6-32K",
        "snappy-32K",
        "zstd_3-64K",
        "gzip_6-64K",
        "snappy-64K",
    ],
    "compr_ratio": [
        26.75,
        25.49,
        36.67,
        26.35,
        25.16,
        36.23,
        25.70,
        24.52,
        35.31,
        25.33,
        24.32,
        34.96,
    ],
    "ins_thr": [
        50.01,
        84.13,
        286.02,
        54.43,
        86.96,
        311.08,
        56.9,
        77.04,
        313.29,
        58.69,
        69.91,
        289.2,
    ],
    "mg_thr": [
        45.47,
        40.62,
        160.24,
        45.55,
        68.41,
        34.07,
        60.85,
        81.15,
        17.98,
        13.68,
        24.24,
        17.8,
    ],
}

symbols = {"zstd": "v", "gzip": "x", "snappy": "$\\lambda$"}
colors = {"4K": "blue", "8K": "orange", "32K": "green", "64K": "red"}

# Create subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.5, 3.75))


# Function to extract the suffix and prefix for symbols and colors
def extract_symbol_and_color(type_str):
    for key in symbols.keys():
        if type_str.split("-")[0].startswith(key):
            symbol = symbols[key]
            break
    for key in colors.keys():
        if type_str.split("-")[1] == key:
            color = colors[key]
            break
    return symbol, color


# Plotting the data
for i in range(len(results["types"])):
    symbol, color = extract_symbol_and_color(results["types"][i])

    # Plot for compression speed vs. compression ratio
    ax1.scatter(
        results["compr_ratio"][i],
        results["ins_thr"][i],
        label=results["types"][i],
        marker=symbol,
        color=color,
    )

    # Plot for random access speed vs. compression ratio
    ax2.scatter(
        results["compr_ratio"][i],
        results["mg_thr"][i],
        label=results["types"][i],
        marker=symbol,
        color=color,
    )

# Set labels and titles
ax1.set_xlabel("Compression ratio (%)")
ax1.set_ylabel("Insertion throughput (MiB/s)")

ax2.set_xlabel("Compression ratio (%)")
ax2.set_ylabel("Random access throughput (MiB/s)")

# Create a combined legend
handles, labels = [], []
for i in range(len(results["types"])):
    symbol, color = extract_symbol_and_color(results["types"][i])
    handle = ax1.scatter([], [], marker=symbol, color=color, label=results["types"][i])
    handles.append(handle)
    labels.append(results["types"][i])

min_compr_ratio = min(results["compr_ratio"])
max_compr_ratio = max(results["compr_ratio"])
curr_xticks = list(ax1.get_xticks())
curr_xticks = [x for x in curr_xticks if x >= (min_compr_ratio + 1)]
ax1.axvline(x=min_compr_ratio, alpha=0.3, color="gray", linestyle="-", zorder=10)
ax2.axvline(x=min_compr_ratio, alpha=0.3, color="gray", linestyle="-", zorder=10)
ax1.set_xticks([min_compr_ratio] + curr_xticks)
ax2.set_xticks([min_compr_ratio] + curr_xticks)

fig.legend(
    handles,
    labels,
    loc="upper center",
    ncol=4,
    bbox_to_anchor=(0.5, 1.2),
)

ax1.text(
    0.5,
    0.4,
    "Better",
    alpha=0.4,
    c="black",
    ha="center",
    va="baseline",
    rotation=-45,
    size=10,
    bbox=dict(alpha=0.4, fc="silver", ec="silver", boxstyle="larrow", lw=1),
    transform=ax1.transAxes,
    zorder=0,
)
ax2.text(
    0.5,
    0.4,
    "Better",
    alpha=0.4,
    c="black",
    ha="center",
    va="baseline",
    rotation=-45,
    size=10,
    bbox=dict(alpha=0.4, fc="silver", ec="silver", boxstyle="larrow", lw=1),
    transform=ax2.transAxes,
    zorder=0,
)


# Show plot
fig.tight_layout(rect=[0, 0, 1, 0.95], w_pad=3.0)
plt.savefig(
    f"bidim_charts-{int(time.time())}.png", format="png", bbox_inches="tight", dpi=120
)
plt.close()
