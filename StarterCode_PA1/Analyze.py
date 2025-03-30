import matplotlib
matplotlib.use("Agg") 
import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_csv("subscriber_data.csv", names=["timestamp", "topic", "latency", "content"], skiprows=1)

high_latency = df[df["latency"] > 0.3]
print(high_latency)

mean_latency = df["latency"].mean()
median_latency = df["latency"].median()

plt.figure(figsize=(10, 5))
plt.hist(df["latency"], bins=20, alpha=0.6, label="Latency Distribution")
#plt.axvline(mean_latency, color="red", linestyle="dashed", linewidth=2, label=f"Mean: {mean_latency:.4f}s")
#plt.axvline(median_latency, color="green", linestyle="dashed", linewidth=2, label=f"Median: {median_latency:.4f}s")

plt.xlabel("Latency (s)")
plt.ylabel("Frequency")
plt.title("Latency Distribution Between Publisher and Subscriber")
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)
#plt.show()
# 直接保存到当前目录
plt.savefig("latency_plot.png", dpi=300)


# avg_latency = df.groupby("topic")["latency"].mean()

# plt.figure(figsize=(10, 5))
# avg_latency.plot(kind="bar", color="skyblue")
# plt.xlabel("Topic")
# plt.ylabel("Average Latency (s)")
# plt.title("Average Latency per Topic")
# plt.xticks(rotation=45)
# plt.grid(axis="y")
# plt.show()
