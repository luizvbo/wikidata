# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Fiding Structure in Wikipedia

# %% [markdown]
# Wikipedia uses templates to standardize articles in the same category and to define a standard for the way specific types of information are displayed (e.g., info boxes). Acording to [Wikipedia](https://en.wikipedia.org/wiki/Help:Template):
#
# > A template is a Wikipedia page created to be included in other pages. Templates usually contain repetitive material that might need to show up on any number of articles or pages. They are commonly used for boilerplate messages, standard warnings or notices, infoboxes, navigational boxes, and similar purposes.
#
# This makes the information more uniform and easy to read. However, in order to extract the information from an article, we have to parse the template. Fortunately, there is a python package that does the extraction magic for us: [wikitextparser](https://pypi.org/project/wikitextparser)

# %% [markdown]
# # Configuration
#
# The parquet version of Wikipedia we generated in the [previous article](../loading-wikipedia-data-with-python) has around 28GB. In order to deal with such amount of data, we are going to use [dask](https://dask.org/).
#
# Dask is an open source library that allows you to load data larger than the memory by working in chunks of data. It also allows parallel processing and provides a nice dashboard to visualize the progress of your tasks.

# %%
import dask.dataframe as dd
import pandas as pd
import wikitextparser as wtp
import re
from collections import Counter

# load/import classes
from dask.distributed import Client

# %%
# set up cluster and workers
client = Client(n_workers=8, 
                threads_per_worker=1,
                memory_limit='3GB')
# Check http://127.0.0.1:8787/status for the dashboard

# %% [markdown]
# # Selecting Relevant Information
#
# In order to filter out articles that we are not interested, we will remove articles with 250 characters or less and articles from [special namespaces]([https://en.wikipedia.org/wiki/Wikipedia:Namespace).

# %%
# %%time
namespaces = ['user', 'wikipedia', 'file', 'mediawiki',
              'template', 'help', 'category', 'portal', 
              'draft', 'timedtext', 'module', 'book']

ddf = dd.read_parquet("wiki_parquet/*.parquet", engine="pyarrow")
ddf = ddf.dropna(subset=['article'])
# Filter out articles smaller than 250 characters
ddf = ddf.loc[ddf['article'].str.len() > 250]
# Filter out Wikipedia namespace pages
ddf = ddf[ddf['title']
          .str.lower()
          .apply(lambda s: sum([s.startswith(ns) for ns in namespaces]) == 0,
                 meta=('title', 'bool'))]


# %% [markdown]
# # Extracting Infoboxes
#
# In order to speed-up next executions, we are going to store the infobox information extracted. It should take around 1.8GB of space in disk. Notice that we are storing only the rows where the infobox is not null.

# %%
# %%time
def get_root_infobox(article):
    parsed = wtp.parse(article)
    templates = []
    for t in parsed.templates:
        if len(t.ancestors()) == 0:
            if t.name.lower().startswith('infobox'):
                templates.append(str(t))
    if len(templates) == 0:
        return None
    return templates

# Extract infoboces at the level 0 
ddf['infobox'] = (
    ddf['article']
    .apply(get_root_infobox, meta=('infobox', 'object'))
)
# Store them into parquet
ddf.loc[ddf['infobox'].notnull(), ['index', 'infobox']].to_parquet('wiki_parquet_infobox')

# %%
# %%time
re_infobox_type = re.compile(r'\{\{infobox[ _](.+?)[|\n(:?<!--)\}]', 
                             flags=re.IGNORECASE)

def get_infobox_type(infoboxes):
    infobox_types = []
    for s_infobox in infoboxes:
        re_res = re_infobox_type.findall(s_infobox)
        if len(re_res) > 0:
            infobox_types.append(re_res[0].lower().strip())
    return infobox_types
        
ddf_infobox_types = dd.read_parquet("wiki_parquet_infobox/*.parquet", engine="pyarrow")
ddf_infobox_types['infobox_type'] = (
    ddf_infobox_types
    .loc[:, 'infobox']
    .apply(get_infobox_type, meta=('infobox_types', 'object'))
)

df_infobox_types = ddf_infobox_types[['index', 'title', 'infobox_type']].compute()

# %%
# df_infobox_types['n_types'] = df_infobox_types.infobox_type.apply(lambda _: len(_))

# %%
cnt = Counter()

for row in df_infobox_types.infobox_type:
    if len(row) > 0:
        for type_ in row:
            cnt[type_.lower()] += 1
            
s_infobox_count = pd.Series(cnt).sort_values(ascending=False)

# %%
# df_infobox_types[lambda _: _.infobox_type.apply(lambda _: 'university' in _)]

# %%
infobox_type_dict = dict(
    person={'person', 'football biography', 'officeholder',
            'musical artist', 'sportsperson', 'scientist',
            'military person', 'writer', 'cricketer',
            'baseball biography',},
    place={'settlement', 'nrhp', 'station', 'french commune',
           'school', 'river', 'uk place', 'university', 
           'road'},
    music={'album', 'song'},
    film={'film', 'television'},
    organization={'company'},
    publication={'book'},
    ship={'ship career', 'ship characteristics', 'ship begin',
          'ship image', },
    sports={'ncaa team season'},
    game={'video game'}
)

# s_infobox_count.to_frame().assign(cumsum=lambda _:_[0].cumsum()).iloc[20:40,:]
def classify_infobox(types):
    classes = set()
    for type_ in types:
        for k, set_ in infobox_type_dict.items():
            if type_ in set_:
                classes.add(k)
    return classes


# %%
(
    df_infobox_types
    .sample(frac=0.1)
    .assign(infobox_class=lambda _: _.infobox_type.apply(classify_infobox))
    .assign(count_class=lambda _: _.infobox_class.apply(lambda _: len(_)))
    .loc[lambda _: _.count_class > 1]
)    

# %%
set_

# %%
# articles = ddf['article'].sample(0.005).compute()

# %%
with open('art_ex_robert_plant.txt', 'r') as f:
    art = wtp.parse(f.read())

# %%
infobox = wtp.parse(str(art.templates[2]))

# %%
infobox.templates[0].name.lower().replace('inf')

# %%
type(art)

# %%
print(str(art))

# %%
len(art.templates[0].ancestors())


# %%
def get_root_infobox(article):
    parsed = wtp.parse(article)
    templates = []
    for t in parsed.templates:
        if len(t.ancestors()) == 0:
            templates.append(t)
    return templates


# %%
import wikitextparser as wtp

def get_infobox(article):
    wp = wtp.parse(article)
    


RE_INFOBOX = r"\{\{infobox (.+)"

RE_INFOBOX_TEST = r"\{\{infobox (.+?)[\n\|(:?<!--)]"

ib_articles = (
    articles
    .to_frame()
    .assign(infobox=lambda _: _['article'].str.extract(RE_INFOBOX, flags=re.IGNORECASE))
    .dropna(subset=['infobox'])
    .assign(infobox_test=lambda _: _['article'].str.extract(RE_INFOBOX_TEST, flags=re.IGNORECASE))
)

# %%
import wikitextparser as wtp

with open('art_ex_robert_plant.txt', 'r') as f:
    s_art = f.read()

wt = wtp.parse(s_art)
