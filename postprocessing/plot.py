import pandas
import matplotlib.pyplot as plt
import numpy as np

measurements = pandas.read_csv("peerstore.csv", quotechar="'")
print(measurements.describe)
print(measurements.nunique())

#data cleanup and Deneb only values
measurements['rttMin_Deneb'] = measurements['rttMin']
measurements.loc[measurements['forkDigest'] != '6a95a1a9', 'rttMin_Deneb'] = np.nan
measurements.loc[measurements['bwMaxMbps'] == 0 , 'bwMaxMbps'] = np.nan
measurements['bwMaxMbps_Deneb'] = measurements['bwMaxMbps']
measurements.loc[measurements['forkDigest'] != '6a95a1a9', 'bwMaxMbps_Deneb'] = np.nan

measurements[['node_id', 'rttMin', 'rttMin_Deneb']].groupby('node_id').last().plot.hist(
    #density=True,
    bins=500,
    figsize=(10, 5), xlim=(0,500),
    ylabel="number of nodes",
    xlabel="RTT distribution, in milliseconds")
plt.savefig('rtt-hist.png')
measurements[['rttMin', 'rttAvg']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=10000,
    figsize=(10, 5), xlim=(0,1000))
plt.savefig('rtt-cdf.png')
measurements[['node_id', 'bwMaxMbps', 'bwMaxMbps_Deneb']].groupby('node_id').last().plot.hist(
    #density=True,
    bins=500, figsize=(10, 5),
    ylabel="number of nodes",
    xlabel="bandwidth distribution, in Mbps")
plt.savefig('bw-hist.png')
measurements[['bwMaxMbps', 'bwAvgMbps']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=500, figsize=(10, 5))
plt.savefig('bw-cdf.png')

def networkMapper(row):
    enr = row[' enr']
    if " eth2:" in enr:
        return "Eth consensus node on " + row['forkDigest']
    elif " eth:" in enr:
        return "Eth execution node"
    elif " opstack:" in enr:
        return "Optimism execution node"
    elif " ssv:" in enr:
        return "SSV ?"
    elif " opera:" in enr:
        return "Opera ?"
    elif " opel:" in enr:
        return "Opel ?"
    elif " c:" in enr:
        return "Portal Network"
    else:
        #print(enr)
        return "Unknown"

print(measurements['forkDigest'].value_counts())
measurements['network'] = measurements.apply(networkMapper, axis=1)

nodeCnt = measurements['node_id'].nunique()
def myFmt(x):
    return '{:.0f}%\n{:.0f}'.format(x, nodeCnt*x/100)
measurements[['node_id', 'network']].groupby('network').nunique().sort_values(
    by='node_id',ascending=False).plot(
        kind='pie', y='node_id', autopct=myFmt,
        legend=False, figsize=(10, 6), ylabel='',
    )
plt.savefig('forkDigest.png')

del measurements

discovery = pandas.read_csv("discovery.csv", quotechar="'")
discovery['network'] = discovery.apply(networkMapper, axis=1)
nodeCnt = len(discovery['network'])
discovery[['node_id', 'network']].groupby('network').nunique().sort_values(
    by='node_id',ascending=False).plot(
        kind='pie', y='node_id', autopct=myFmt,
        legend=False, figsize=(10, 6), ylabel='',
    )
plt.savefig('forkDigestAll.png')
