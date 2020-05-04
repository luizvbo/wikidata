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
# # To be defined...

# %% [markdown]
# Wikipedia uses templates to standardize articles in the same category and to define a standard for the way specific types of information are displayed (e.g., info boxes). Acording to [Wikipedia](https://en.wikipedia.org/wiki/Help:Template):
#
# > A template is a Wikipedia page created to be included in other pages. Templates usually contain repetitive material that might need to show up on any number of articles or pages. They are commonly used for boilerplate messages, standard warnings or notices, infoboxes, navigational boxes, and similar purposes.
#
# This makes the information more uniform and easy to read. However, in order to extract the information from an article, we have to parse the template. Fortunately, there is a python package that does the extraction magic for us: [wikitextparser](https://pypi.org/project/wikitextparser)

# %%
import dask.dataframe as dd
import pandas as pd
import wikitextparser as wtp
import re

# load/import classes
from dask.distributed import Client

# %%
# set up cluster and workers
client = Client(n_workers=8, 
                threads_per_worker=1,
                memory_limit='3GB')
# Check http://127.0.0.1:8787/status for the dashboard

# %% [markdown]
# ## Selecting relevant articles
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
ddf.loc[ddf['infobox'].notnull(), ['title', 'infobox']].to_parquet('wiki_parquet_infobox')

# %%
# %%time
re_infobox_type = re.compile(r'\{\{infobox (.+?)[|\n(:?<!--)]', 
                             flags=re.IGNORECASE)

def get_infobox_type(infoboxes):
    infobox_types = []
    for s_infobox in infoboxes:
        re_res = re_infobox_type.findall(s_infobox)
        if len(re_res) > 0:
            infobox_types.append(re_res[0])
    return infobox_types
        
ddf_infobox_types = dd.read_parquet("wiki_parquet_infobox/*.parquet", engine="pyarrow")
ddf_infobox_types['infobox_type'] = (
    ddf_infobox_types
    .loc[:, 'infobox']
    .apply(get_infobox_type, meta=('infobox_types', 'object'))
)

df_infobox_types = ddf_infobox_types[['title', 'infobox_type']].compute()

# %%


df_infobox_types

# %%
from collections import Counter

cnt = Counter()

for row in ddf_infobox_types:
    if len(row) > 1:
        cnt[row[0]] += 1

# %%
cnt

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
