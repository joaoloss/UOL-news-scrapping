"""
João Loss - joao.loss@edu.ufes.br

This script processes UOL archive links collected by archive_links_extraction.py and saved in ARCHIVE_CSV_PATH
to extract UOL web news links, which are then saved in OUTPUT_FILES_PATH.
"""

from bs4 import BeautifulSoup
import requests
from requests.exceptions import RequestException
import pandas as pd
import os
import json
import time

ARCHIVE_CSV_PATH = os.path.join("out", "archive_links.csv")
OUTPUT_FILES_PATH = os.path.join("out", "uol_links")
os.makedirs(OUTPUT_FILES_PATH, exist_ok=True)

def save_uol_news_links(archive_links:list[str], year:int):
    def get_real_url_date(url):
        """
        Return the actual year and month from the URL, as it may differ from those in the original archive URL.
        """
        
        date = url.split("/web/")[1][:8]
        y = int(date[:4])
        m = int(date[4:6])
        return y, m
    href_filter = lambda u: "http" + u.split("http")[-1]
    
    # Create the corresponding folder
    year_folder_path = os.path.join(OUTPUT_FILES_PATH, str(year))
    os.makedirs(year_folder_path, exist_ok=True)

    for i, link in enumerate(archive_links):
        for n_try in range(3):
            try:
                response = requests.get(link, timeout=20)
                break
            except RequestException as e:
                print(f"RequestException for {link} (attempt {n_try+1}): {e}")
                time.sleep(5)
        else:
            print(f"Failed to connect with {link} after {n_try+1} attempts. Skipping...")
            continue

        if response.status_code != 200:
            print(f"Response status code != 200 for {link} (got {response.status_code}), skipping.")
            continue

        html = response.text
        actual_year, actual_month = get_real_url_date(response.url)
        soup = BeautifulSoup(html, 'html.parser')
        anchor_elements = soup.find_all("a")

        uol_links = list()
        for e in anchor_elements:
            href = e.get("href")
            if href is not None:
                href = str(href)
                if f"/{actual_year}/" in href:
                    uol_links.append(href_filter(href))
        
        print(f"[{i+1}] {len(uol_links)} links found in {link}.")

        file_path = os.path.join(year_folder_path, f"{actual_month}-{actual_year}.txt")
        if year != actual_year:
            adjusted_year_folder_path = os.path.join(OUTPUT_FILES_PATH, str(actual_year))
            os.makedirs(adjusted_year_folder_path, exist_ok=True)
            file_path = os.path.join(adjusted_year_folder_path, f"{actual_month}-{actual_year}.txt")
            
        with open(file_path, 'a') as f:
            f.write("\n".join(uol_links) + "\n")

def main():
    archive_df = pd.read_csv(filepath_or_buffer=ARCHIVE_CSV_PATH)
    archive_df["links"] = [json.loads(s) for s in archive_df["links"]] # from str to list

    grouped_archive_df = archive_df.groupby(by="year")
    start_time = time.time()
    for year, year_df in grouped_archive_df:
        print(f"==> Year {year}:")  

        for row in year_df.itertuples(index=False, name="row"):
            print(f"Month {row.month} ({len(row.links)} links to precess):")
            save_uol_news_links(row.links, int(row.year))
    end_time = time.time()

    print(f"Time taken to complete: {end_time - start_time:.02f}s.")       

if __name__ == "__main__":
    main()

"""
Examples of archive page achor elements:
<a href="https://web.archive.org/web/20190602000119/https://talesfaria.blogosfera.uol.com.br/2019/06/01/nao-existem-ateus-no-stf-e-a-toa-e-que-nao-existe-brinca-marco-aurelio/" data-uol-see-later="url" name="chamada-destaque-submanchete|coluna-1" data-metrics="mod-1;topo-hibrido"> <span class="uol-see-later" data-service="true" data-share="true" data-id="publisher-7ab97d2a1b73ef9a6aacce6a76f4920190601" data-repository="blog"></span> <figure class="image "> <img data-uol-see-later="image" width="168" height="168" src="https://web.archive.org/web/20190602000119im_/https://conteudo.imguol.com.br/c/home/88/2018/12/19/o-ministro-do-stf-supremo-tribunal-federal-marco-aurelio-mello-participa-de-sessao-plenaria-da-suprema-corte-brasileira-1545237313676_300x168.jpg" data-src="https://web.archive.org/web/20190602000119/https://conteudo.imguol.com.br/c/home/88/2018/12/19/o-ministro-do-stf-supremo-tribunal-federal-marco-aurelio-mello-participa-de-sessao-plenaria-da-suprema-corte-brasileira-1545237313676_300x168.jpg" alt="Carlos Humberto/SCO/STF - 3.set.2015" title="Carlos Humberto/SCO/STF - 3.set.2015" class="lazyload loaded"> </figure> <strong class="chapeu color1">Corte e religião</strong> <h2 class="titulo color2" data-uol-see-later="title">T. Faria: Não existem ateus no STF. 'E à toa é que não existe', brinca Marco Aurélio <span class="comentariosContainer"></span></h2> </a>
<a name="vA-semfoto3-manchete" href="https://web.archive.org/web/20110903043449/http://click.uol.com.br/?rf=home-vA-semfoto3-manchete&amp;u=http://esporte.uol.com.br/futebol/ultimas-noticias/2011/09/02/depois-de-ser-expulso-do-banco-de-reservas-valdivia-e-furtado-em-hotel-na-suica.htm" onclick="s_objectID=&quot;http://click.uol.com.br/?rf=home-vA-semfoto3-manchete&amp;u=http://esporte.uol.com.br/futebol/ultimas_2&quot;;return this.s_oc?this.s_oc(e):true">Após ser expulso do banco de reservas, Valdivia é furtado na Suíça</a>
<a name="vA-fotomedia-manchete" href="https://web.archive.org/web/20100104095858/http://click.uol.com.br/?rf=home-vA-fotomedia-manchete&amp;u=http://televisao.uol.com.br/ultimas-noticias/2010/01/04/ult4244u4326.jhtm">"Achei que não ia dar conta", diz Assunção sobre novo papel na TV</a>
"""