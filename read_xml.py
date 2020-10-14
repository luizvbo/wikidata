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
import sys
import re
import logging

logging.getLogger().setLevel(logging.INFO)

url_wikidump = "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/enwiki/20200920/"


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

def wiki_xml_to_parquet(path_xml, output_folder=None, max_memory=500000):
    def get_article(article):
        ns = {'mw': 'http://www.mediawiki.org/xml/export-0.10/'}

        return (
            article.xpath('./mw:id', namespaces=ns)[0].text,
            article.xpath('./mw:title', namespaces=ns)[0].text,
            article.xpath('./mw:revision/mw:text', namespaces=ns)[0].text
        )

    def free_articles(articles):
        for art in articles:
            del art
        del articles

    path_xml = Path(path_xml)
    if output_folder is None:
        output_path = path_xml
    else:
        output_path = Path(output_folder).joinpath(path_xml.name)

    page_tag = '{http://www.mediawiki.org/xml/export-0.10/}page'
    articles = []
    file_counter = 0
    mem_size = 0

    for _, art in tqdm(etree.iterparse(str(path_xml), tag=page_tag)):
        new_article = get_article(art)
        articles.append(new_article)
        mem_size += sys.getsizeof(new_article[2])
        art.clear()
        # Eliminate empty references from the root node to elem
        for ancestor in art.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]

        batch_counter += 1
        if mem_size >= max_memory:
            parquet_path = output_path.parent / (output_path.name + '_{:03d}.parquet'.format(file_counter))
            logging.info(f"\nWriting to file {parquet_path}")
            pd.DataFrame(articles, columns=['id', 'title', 'content']).to_parquet(parquet_path)

            free_articles(articles)

            articles = []
            file_counter += 1
            mem_size = 0
    if len(articles) > 0:
        parquet_path = output_path.parent / (output_path.name + '_{:03d}.parquet'.format(file_counter))
        logging.info(f"\nWriting to file {parquet_path}")
        pd.DataFrame(articles, columns=['id', 'title', 'content']).to_parquet(parquet_path)
        free_articles(articles)


# %%
def make_database(root_folder):
    df_links = (
        pd.read_html(url_wikidump)[0][['Name', 'Last modified', 'Size']]
        .dropna(how='all')
        .loc[lambda df: df.Name.apply(lambda el: re.match(
            'enwiki-20200920-pages-articles\d+\.xml', el) is not None
        )]
        .set_index('Name')
    )

    bz2_files = set([_.rsplit('/', 1)[-1] for _ in glob(os.path.join(root_folder, 'bz2/*bz2'))])
    xml_files = set([_.rsplit('/', 1)[-1] + '.bz2'
                     for _ in glob(os.path.join(root_folder, 'xml/*xml*'))])
    prq_files = set([re.sub('_\d{3}\.parquet', '.bz2', _.rsplit('/', 1)[-1])
                     for _ in glob(os.path.join(root_folder, 'parquet/*parquet'))])

    df_links.loc[bz2_files, 'bz2'] = True
    df_links.loc[xml_files, 'xml'] = True
    df_links.loc[prq_files, 'parquet'] = True

    return df_links.fillna(False)


# %%
df = make_database('articles')
root_folder = 'articles'

for i, row in df.head(5).iterrows():
    logging.info(f'Processing {i}')

    _xml_path = lambda f: os.path.join(root_folder, 'xml', f.rsplit('.', 1)[0])
    _bz2_path = lambda f: os.path.join(root_folder, 'bz2', f)

    if row['xml'] or row['parquet']:
        # Delete the bz2 file
        shutil.rmtree(_bz2_path(i), ignore_errors=True)
    if row['parquet']:
        # Delete the xml file
        shutil.rmtree(_xml_path(i), ignore_errors=True)
    if not row['parquet']:
        if not row['xml']:
            #TODO: Add `if not row['bz2']`
            logging.info(f"Extracting {i}")
            extract_bz2(_bz2_path(i), os.path.join(root_folder, 'xml'))
            # Delete the bz2 file
            shutil.rmtree(_bz2_path(i), ignore_errors=True)

        logging.info(f"Converting {i.rsplit('.', 1)[0]} to parquet")
        wiki_xml_to_parquet(_xml_path(i), os.path.join(root_folder, 'parquet'))
        # Delete the xml file
        shutil.rmtree(_xml_path(i), ignore_errors=True)

    logging.info(f'Done!')

# %%
# df = make_database('articles')
# df.head()

# %%
