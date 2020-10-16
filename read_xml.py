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
from pathlib import Path
import pandas as pd
from lxml import etree
from glob import glob
from tqdm import tqdm
import shutil
from typing import Optional
import bz2
import os
import sys
import re
import logging

logging.getLogger().setLevel(logging.INFO)

url_wikidump = (
    "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/enwiki/20200920/"
)


# %%
def extract_bz2(path_bz2: str, output_folder: Optional[str] = None):
    """Extract a Wikipedia bz2 file.

    Args:
        path_bz2 (str): Path to the bz2 file
        output_folder (Optional[str], optional): Path to the folder where to
            extract the file. If the output folder is not provided, it extracts
            to the folder containing the input file. Defaults to None.
    """
    path_bz2 = Path(path_bz2)
    assert path_bz2.suffix == '.bz2'
    if output_folder is None:
        output_path = path_bz2.parent.joinpath(path_bz2.stem)
    else:
        output_path = Path(output_folder).joinpath(path_bz2.stem)
    with bz2.BZ2File(path_bz2) as fr, open(output_path, "wb") as fw:
        shutil.copyfileobj(fr, fw, length=2000000)


def wiki_xml_to_parquet(path_xml: str, output_folder: Optional[str] = None,
                        max_memory: int = 250):
    """Convert a Wikipedia XML file into parquet files

    Args:
        path_xml (str): Path to the xml file
        output_folder (Optional[str], optional): Path to the folder where to
            store the files. If the output folder is not provided, it extracts
            to the folder containing the input file. Defaults to None.
        max_memory (int): Rough estimate of size in memory (in MB) before start
            writing to file. When the size of the list of articles in memory
            reach this number, a new parquet file is written. Defaults to 250.
    """
    def get_article(article):
        """Convert an lxml object of an article to a tuple"""
        ns = {'mw': 'http://www.mediawiki.org/xml/export-0.10/'}

        return (
            int(article.xpath('./mw:id', namespaces=ns)[0].text),
            article.xpath('./mw:title', namespaces=ns)[0].text,
            article.xpath('./mw:revision/mw:text', namespaces=ns)[0].text
        )

    def free_articles(articles):
        """Free resource used by the list of articles."""
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
    # Convert MB to Bytes
    max_memory = max_memory * 1024**2
    mem_size = 0

    for _, art in tqdm(etree.iterparse(str(path_xml), tag=page_tag)):
        new_article = get_article(art)
        articles.append(new_article)
        # Increment the variable by the size of article content (in bytes)
        mem_size += sys.getsizeof(new_article[2])
        art.clear()
        # Eliminate empty references from the root node to elem
        for ancestor in art.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]

        if mem_size >= max_memory:
            parquet_path = (
                output_path.parent /
                (output_path.name + '_{:03d}.parquet'.format(file_counter))
            )
            logging.info(f"\nWriting to file {parquet_path}")
            pd.DataFrame(articles, columns=['id', 'title', 'content'])\
                .to_parquet(parquet_path)

            free_articles(articles)

            articles = []
            file_counter += 1
            mem_size = 0
    if len(articles) > 0:
        # Write the last articles to file
        parquet_path = (
            output_path.parent /
            (output_path.name + '_{:03d}.parquet'.format(file_counter))
        )
        logging.info(f"\nWriting to file {parquet_path}")
        pd.DataFrame(articles, columns=['id', 'title', 'content'])\
            .to_parquet(parquet_path)
        free_articles(articles)


# %%
def make_database(root_folder):
    df_links = (
        pd.read_html(url_wikidump)[0][['Name', 'Last modified', 'Size']]
        .dropna(how='all')
        .loc[lambda df: df.Name.apply(lambda el: re.match(
            r'enwiki-20200920-pages-articles\d+\.xml', el) is not None
        )]
        .set_index('Name')
    )

    bz2_files = set([_.rsplit('/', 1)[-1]
                     for _ in glob(os.path.join(root_folder, 'bz2/*bz2'))])
    xml_files = set([_.rsplit('/', 1)[-1] + '.bz2'
                     for _ in glob(os.path.join(root_folder, 'xml/*xml*'))])
    prq_files = set(
        [re.sub(rf'_\d{3}\.parquet', '.bz2', _.rsplit('/', 1)[-1])
         for _ in glob(os.path.join(root_folder, 'parquet/*parquet'))]
    )

    df_links.loc[bz2_files, 'bz2'] = True
    df_links.loc[xml_files, 'xml'] = True
    df_links.loc[prq_files, 'parquet'] = True

    return df_links.fillna(False)


# %%
def process_bz2_folder(root_folder, delete_xml=True, delete_bz2=False):
    def _xml_path(f):
        return os.path.join(root_folder, 'xml', f.rsplit('.', 1)[0])

    def _bz2_path(f):
        return os.path.join(root_folder, 'bz2', f)

    def _remove_file(path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    df_articles = make_database(root_folder)
    for i, row in df_articles.iterrows():
        logging.info(f'Processing {i}')
        if delete_bz2 and (row['xml'] or row['parquet']):
            # Delete the bz2 file
            _remove_file(_bz2_path(i))
        if row['parquet'] and delete_xml:
            # Delete the xml file
            _remove_file(_xml_path(i))
        else:
            if not row['xml']:
                # TODO: Add `if not row['bz2']`
                logging.info(f"Extracting {i}")
                extract_bz2(_bz2_path(i), os.path.join(root_folder, 'xml'))
                if delete_bz2:
                    # Delete the bz2 file
                    _remove_file(_bz2_path(i))

            logging.info(f"Converting {i.rsplit('.', 1)[0]} to parquet")
            wiki_xml_to_parquet(_xml_path(i),
                                os.path.join(root_folder, 'parquet'))
            if delete_xml:
                # Delete the xml file
                _remove_file(_xml_path(i))

        logging.info('Done!')

# %%
process_bz2_folder('/run/media/luiz/Elements/wikifiles/')
