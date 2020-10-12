# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from lxml import etree
from glob import glob
import bz2 
from tqdm import tqdm
import shutil
from pathlib import Path
import os


# %%
def extract_bz2(path_bz2, output_folder=None):
    path_bz2 = Path(path_bz2)
    assert path_bz2.suffix == '.bz2'
    if output_folder is None:
        output_path = path_bz2.parent.joinpath(path_bz2.stem)
    else:
        output_path = Path(output_folder).joinpath(path_bz2.stem)
    with bz2.BZ2File(path_bz2) as fr, open(output_path, "wb") as fw:
        shutil.copyfileobj(fr, fw, length = 1000000)
        
def wiki_xml_to_parquet(path_xml):
    def get_article(article):
        return (
            article.xpath('./mw:id', namespaces=ns)[0].text,
            article.xpath('./mw:title', namespaces=ns)[0].text, 
            article.xpath('./mw:revision/mw:text', namespaces=ns)[0].text
        )
    
    ns = {'mw': 'http://www.mediawiki.org/xml/export-0.10/'}
    page_tag = '{http://www.mediawiki.org/xml/export-0.10/}page'
    df_articles = []
    
    for _, el in tqdm(etree.iterparse(path_xml, tag=page_tag)):
        df_articles.append(get_article(art))
        
#     root = etree.parse(path_xml).getroot()
#     articles = root.xpath('//mw:page', namespaces=ns)
    df_articles = pd.DataFrame(
        df_articles,
#         [get_article(art) for art in tqdm(articles)],
        columns=['id', 'title', 'content']
    )
    return df_articles
#     df_articles.to_parquet(path_xml + '.parquet')


# %%
wiki_xml_to_parquet('articles/enwiki-20200920-pages-articles10.xml-p4045403p5399366')

# %%
ns={'mw': 'http://www.mediawiki.org/xml/export-0.10/'}
for event, element in etree.iterparse('articles/enwiki-20200920-pages-articles1.xml-p1p41242', tag='{http://www.mediawiki.org/xml/export-0.10/}page'):
    print(element)
    break

# %%
element.xpath('./mw:title', namespaces=ns)[0].text
