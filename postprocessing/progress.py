import pandas
import matplotlib.pyplot as plt

logs = pandas.read_json("dcrawl.log", lines=True)
print(logs.describe)

progress = logs[(logs['msg'] == 'findNode finished') & (logs['ts2'] >= 0) & (logs['queued'] >= 0.0)]
progress = progress[['ts2', 'queued', 'measured', 'failed']]
progress['total'] = progress['queued'] + progress['measured'] + progress['failed']
progress['ts2']/= 1000
print(progress.describe)
progress.to_csv('test.csv')
progress.plot(x='ts2',
              figsize=(10, 5),
              title='measurement progress',
              xlabel='time since start [seconds]',
              #xlim=(0,600)
              )

plt.savefig('progress.png')
