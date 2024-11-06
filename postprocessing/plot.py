import pandas
import matplotlib.pyplot as plt

measurements = pandas.read_csv("peerstore.csv", quotechar="'")
print(measurements.describe)
print(measurements.nunique())

measurements[['rttMin', 'rttAvg']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=10000,
    figsize=(10, 5), xlim=(0,1000))
plt.savefig('rtt-cdf.png')
measurements[['bwMaxMbps', 'bwAvgMbps']].plot.hist(
    cumulative=True, density=True, histtype='step', bins=10000, figsize=(10, 5))
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
        return "C ?"
    else:
        #print(enr)
        return "Unknown"

print(measurements['forkDigest'].value_counts())
measurements['network'] = measurements.apply(networkMapper, axis=1)

nodeCnt = len(measurements['network'])
def myFmt(x):
    return '{:.0f}%\n{:.0f}'.format(x, nodeCnt*x/100)
measurements[['node_id', 'network']].groupby('network').nunique().sort_values(
    by='node_id',ascending=False).plot(
        kind='pie', y='node_id', autopct=myFmt,
        legend=False, figsize=(10, 6), ylabel='',
    )
plt.savefig('forkDigest.png')
