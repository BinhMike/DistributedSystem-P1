import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("latency_results.csv", names=["topic", "time", "latency"])


plt.figure(figsize=(10, 5))
plt.hist(df["latency"], bins=20, alpha=0.6, label="Latency Distribution")
plt.xlabel("Latency (s)")
plt.ylabel("Frequency")
plt.title("Latency Distribution Between Publisher and Subscriber")
plt.legend()
plt.grid()
plt.show()


# avg_latency = df.groupby("topic")["latency"].mean()

# plt.figure(figsize=(10, 5))
# avg_latency.plot(kind="bar", color="skyblue")
# plt.xlabel("Topic")
# plt.ylabel("Average Latency (s)")
# plt.title("Average Latency per Topic")
# plt.xticks(rotation=45)
# plt.grid(axis="y")
# plt.show()
