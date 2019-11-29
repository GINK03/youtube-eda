import Parser
import pandas as pd
import numpy as np
df = pd.read_csv('./vars/channels.csv')

Parser.parse_km_subs(df)
Parser.parse_estimate(df)


print(df.sort_values(by=['subs'], ascending=False).head(20))
import seaborn as sns
from matplotlib import pyplot as plt

plt.figure(figsize=(30, 30))

df['subs'] = np.log1p(df.subs)
df['estimate'] = np.log1p(df.estimate)
ax = sns.scatterplot(x='subs', y='estimate', data=df)
ax.set_title(f'relation of subscribers and estiamte monthly earn(JP YEN) n={len(df)}', 
                fontsize=35)
ax.set_xlabel('subs(log-scale)', fontsize=20)
ax.set_ylabel('estimate monthly earn(log-scale)', fontsize=20)

fig = ax.get_figure()
fig.savefig(f'imgs/fig.png')
fig.clf()
