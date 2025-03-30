import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("latency_results.csv", names=["topic", "time", "latency"])


plt.figure(figsize=(10, 5))
plt.hist(df["latency"], bins=20, alpha=0.6, label="Latency Distribution")
plt.xlabel("Latency (s)")
plt.ylabel("Frequency")
plt.title("Latency Distribution Between Publisher and Subscriber")
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)
#plt.show()
plt.savefig("latency_plot.png", dpi=300)


