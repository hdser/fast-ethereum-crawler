import pandas
import matplotlib.pyplot as plt

measurements = pandas.read_csv("peerstore.csv")
print(measurements.describe)
print(measurements.nunique())

measurements[['rttMin', 'rttAvg']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=10000,
    figsize=(10, 5), xlim=(0,1000))
plt.savefig('rtt-cdf.png')
measurements[['bwMaxMbps', 'bwAvgMbps']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=10000, figsize=(10, 5))
plt.savefig('bw-cdf.png')
