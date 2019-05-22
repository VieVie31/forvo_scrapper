import os
import sys
import base64
import hashlib
import requests

import parsel
import numpy as np
import pandas as pd

from pathlib import Path
from urllib.parse import unquote

from tqdm import tqdm
from numpy import genfromtxt
from bs4 import BeautifulSoup

LANG = sys.argv[1]
assert LANG in "de tt ru en es ja fr pt pl nl it zh grc sv tr ar hu ko cs lb el uk ca he fa fi chm yue ur eo da bg la ro nan lt no vi is hr ga eu wuu be lv ba hi sk nds pa kbd sr hak ug az th ind et sl tl vec sme yi gl bn af cy ia cv hy ku oc mk uz lmo ta kmr arz udm sw tly sq kk bs ka ast br cdo sa vls mr gd gan scn ce nah fo te gsw apc jv ms mn pms nap hsn sco za fy ml tlh ceb bar zza gn sc ps pi mt gv swg cjy frp nn lkt cr trv haw ady gu ig bo vo".split()
CSV_INDEX = './scrapped/{}.csv'.format(LANG)
BASE_URL = "https://fr.forvo.com/languages-pronunciations/{}/page-{}/"
BASE_FIRST_URL = "https://fr.forvo.com/languages-pronunciations/{}/"
BASE_AUDIO_URL = "https://audio00.forvo.com/audios/mp3/{}"


# Create the scrapped content directory if not exists
try:
    os.mkdir('./scrapped/')
except FileExistsError:
    pass

try:
    os.mkdir('./scrapped/{}/'.format(LANG))
except FileExistsError:
    pass

Path(CSV_INDEX).touch()

def process_not_fetched(url):
    print("... Page not fetched : {}".format(url))

class WordPage:
    def __init__(self, word_page_url):
        self.url = word_page_url
        self.word = unquote(word_page_url.split('/word/')[-1].split('/')[0])
        self.words_urls = None
    
    def fetch(self):
        if self.words_urls != None:
            return self.words_urls
        r = requests.get(self.url)
        if not r.ok:
            process_not_fetched(self.url)
        html_page = BeautifulSoup(r.text, "lxml")
        tree = parsel.Selector(html_page.prettify())
        # Get the audio files url
        tr_lst = tree.xpath('//*[@id="language-container-{}"]/article[1]/ul/li/span'.format(LANG))
        tr_lst = list(filter(lambda e: 'id' in e.attrib and e.attrib['id'][:5] == "play_", tr_lst))
        self.words_urls = list(map(
            lambda e: BASE_AUDIO_URL.format(base64.b64decode(e.attrib['onclick'].split(",'")[3][:-1]).decode()),
            tr_lst
        ))

        # Save the audio files
        try:
            csv_index = list(pd.read_csv(CSV_INDEX, header=None).as_matrix())
        except pd.errors.EmptyDataError:
            csv_index = []
        for word_url in self.words_urls:
            r = requests.get(word_url)
            if not r.ok:
                process_not_fetched(word_url)
                continue
            audio_md5 = hashlib.md5(r.content).hexdigest()
            f = open('./scrapped/{}/{}.mp3'.format(LANG, audio_md5), 'wb')
            f.write(r.content)
            f.close()
            csv_index.append((str(audio_md5), str(self.word)))
        
        # Update the index
        pd.DataFrame(csv_index).to_csv(CSV_INDEX, index=False, header=False)
        return self.words_urls


for page_idx in tqdm(range(1, 21)):
    # Create the page url to fetch
    page_url = BASE_URL.format(LANG, page_idx) if page_idx > 1 else BASE_FIRST_URL.format(LANG)

    # Fetch the page
    r = requests.get(page_url)
    if not r.ok:
        process_not_fetched(page_url)

    # Parse the page to get the listing of words pages
    html_page = BeautifulSoup(r.text, "lxml")
    tree = parsel.Selector(html_page.prettify())
    tr_lst = tree.xpath('//*[@id="displayer"]/div/section/div/ul/li/a')
    word_pages = list(map(lambda e: e.attrib['href'], tr_lst))

    for word_page in tqdm(word_pages):
        WordPage(word_page).fetch()
    
