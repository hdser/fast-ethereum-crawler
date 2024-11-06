import pandas as pd
import matplotlib.pyplot as plt

logs = pd.read_json("dcrawl.log", lines=True)
print(logs.describe)

progress = logs[(logs['msg'] == 'findNode finished') & (logs['ts2'] >= 0) & (logs['queued'] >= 0.0)]
#progress = progress[['ts2', 'queued', 'measured', 'failed']]
progress['total'] = progress['queued'] + progress['measured'] + progress['failed']
progress['ts2']/= 1000
progress['ts2_bin'] = round(progress['ts2'], ndigits=0)

progress.plot(x='ts2', y=['queued', 'measured', 'failed', 'discovered', 'pending'],
              figsize=(10, 5),
              title='measurement progress',
              xlabel='time since start [seconds]',
              #xlim=(0,600)
              )
plt.savefig('progress.png')

print(progress[['ts2_bin', 'new']].groupby('ts2_bin').mean())
progress[['ts2_bin', 'new']].groupby('ts2_bin').mean().plot(y=['new'],
              figsize=(10, 5),
              title='new nodes discovered',
              xlabel='time since start [seconds]',
              #xlim=(0,600)
              )
plt.savefig('new.png')
