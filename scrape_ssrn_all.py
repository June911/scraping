"""
Scraping all relevant information on ssrn papers in the Financial Economic Network
- there are 200,000 urls, we use the Scraper API, as it is more stable
- Scarper API: https://www.scraperapi.com/documentation/

"""

import os
import csv
import json
import time
import requests
import concurrent.futures

from bs4 import BeautifulSoup
from tqdm import tqdm
from ordered_set import OrderedSet
from urllib.parse import urlencode

"""
SCRAPER SETTINGS
You need to define the following values below:
- API_KEY --> Find this on your dashboard, or signup here to create a 
                free account here https://dashboard.scraperapi.com/signup

- NUM_RETRIES --> We recommend setting this to 5 retries. For most sites 
                95% of your requests will be successful on the first try,
                and 99% after 3 retries. 

- NUM_THREADS --> Set this equal to the number of concurrent threads available
                in your plan. For reference: Free Plan (5 threads), Hobby Plan (10 threads),
                Startup Plan (25 threads), Business Plan (50 threads), 
                Enterprise Plan (up to 5,000 threads).
"""
API_KEY = '1db367e66ef9babdb1e8f1ee29f4c98f'
NUM_RETRIES = 3
NUM_THREADS = 10


def quickSoup(url):
    """
    Send request and return soup
    with the help of scrap API

    :param url:
    :return:
    """
    params = {'api_key': API_KEY, 'url': url}

    response = ""
    # send request to scraperapi, and automatically retry failed requests
    for _ in range(NUM_RETRIES):
        try:
            response = requests.get('http://api.scraperapi.com/', params=urlencode(params))
            if response.status_code in [200, 404]:
                ## escape for loop if the API returns a successful response
                break
        except requests.exceptions.ConnectionError:
            response = ''

    ## parse data if 200 status code (successful response)
    if response.status_code == 200:
        ## parse data with beautifulsoup
        soup = BeautifulSoup(response.content, "html.parser")

        ##
        if "Page Cannot be Found" in soup.get_text():
            return None

        return soup

    elif response.status_code == 404:
        print("not found url")
        return None

    else:
        return None


def find_info_in_one_paper(url):
    """
    find relevant info in the paper url
    :param url:
    :return:
    """
    # request url and parse
    soup = quickSoup(url)

    if soup:
        try:
            # the main body
            body = soup.find(class_="container abstract-body")

            # the list of text
            text_list = OrderedSet(soup.get_text().split("\n")) - {''}

            # find title
            title = body.find('h1').get_text().replace("\n", "")

            # find author
            authors = text_list[0].replace(title, "").replace(" :: SSRN", "").replace(" by ", "")

            # find abstract
            # need to add quotation mark for conveniently storing in csv
            abstract = "\"{}\"".format(body.find(class_="abstract-text").get_text().replace("\n", "").replace("Abstract", ""))

            # find journal
            if body.find(class_="reference-info"):
                journal = "\"{}\"".format(body.find(class_="reference-info").get_text().replace("\n", ""))
            else:
                journal = " "

            # find date ==> post date, last revisit date
            date = body.find(class_="note note-list").get_text().replace("\n", ", ")
            if "Pages" in date:
                date = date.split("Pages, ")[1]
            else:
                date = date.replace(", ", "")

            # find date ==> writen day
            date_written = [line for line in text_list if "Date Written" in line]
            if len(date_written) > 0:
                date_written = date_written[0]
            date = date + str(date_written)

            # find university
            universities = body.find(class_="authors authors-full-width").find_all("p")
            universities = [university.get_text() for university in universities]

            # convert list to string with ";" as seperator
            universities = "\"{}\"".format(",".join(universities))

            # find paper statistics
            stats = OrderedSet(body.find('div', attrs={'class': 'box-paper-statics'}).get_text().split("\n"))

            views, dl, rank, n_refs, n_cit = "", "", "", "", ""
            try:
                views = stats[stats.index('Abstract Views') + 1].strip().replace(",", "")
            except Exception as er:
                # print(er)
                # print("views error")
                pass

            try:
                dl = stats[stats.index('Downloads') + 1].strip().replace(",", "")
            except Exception as er:
                # print(er)
                # print("dl error")
                pass

            try:
                rank = stats[stats.index('rank') + 1].strip().replace(",", "")
            except Exception as er:
                # print(er)
                # print("rank error")
                pass

            # reference
            try:
                refs = body.find(class_="references-citations").get_text().split()
                n_refs = [n for n in refs if n.isdigit()][0]
            except Exception as er:
                # print(er)
                # print("reference error")
                pass
            # citations
            try:
                # return a link
                cit = body.find(id="citations-widget-abstract")["data-url"]
                # request the link
                soup_cit = quickSoup(cit)
                # find number of citations
                n_cit = str(json.loads(soup_cit.get_text())["total_items"])
            except Exception as er:
                # print(er)
                # print("citations error")
                pass

            # combine all results
            results = [url, title, abstract, authors, journal, date, universities, views, dl, rank, n_refs, n_cit]
            return results

        except Exception as es:
            print(es)
            return [url] + [","] * 11

    else:
        print("soup ==> None")
        return [url] + [","] * 11


def find_lst_paper(url_section, get_total=False):
    """
    # find the urls of all papers in one url in one section

    :param url_section:
    :param get_total:
    :return:
    """

    # request url and parse the html
    # get the web
    soup = quickSoup(url_section)

    if soup:
        # find the body that contains all url
        body = soup.find(class_="tbody")

        # find the urls
        title_url = body.find_all(class_="title optClickTitle", href=True)

        # transform into list
        lst_title_url = [i["href"] for i in title_url]

        # get total number of pages
        if get_total:
            if soup.find(class_="results-header").find(class_="total"):
                n_total = soup.find(class_="results-header").find(class_="total").get_text()
            else:
                # may not one page
                n_total = 1
            return lst_title_url, n_total
        else:
            return lst_title_url
    else:
        return []


def get_link_for_all_section_in_one_topic(url):
    """
    Get the url and name for all sections in one topic
    :param url: topic url in ssrn,
                eg. Financial Economics Network: https://www.ssrn.com/index.cfm/en/fen/
    :return: a list of dictionary
             eg. [{"url":"....", "name": "....}, {}, {}]
    """
    # request url and parse the html
    # get the web
    soup = quickSoup(url)

    # find the body that contains all url
    body = soup.find("div", id="network-subject-areas")
    # find the topic url
    topic_url = body["data-url"]
    # access again the topic url
    soup = quickSoup(topic_url)
    # find the list of urls
    lst_url = json.loads(str(soup))["journals"]

    return lst_url


def find_all_urls_in_section(url_section, name_section):
    """
    Get the urls for all papers in one section

    :param url_section: str, url for one section in one topic
    :param name_section: str, name of the topic
    :return:
    """

    # split the url to get the common parts
    url_splited = url_section.split(".cfm")
    base_url = url_splited[0] + ".cfm"
    base_url1 = url_splited[1].replace("?", "&")

    # url in the first page
    url_section_first_page = base_url + "?npage={}".format(1) + base_url1 + "&Network=no&lim=false"
    # list of all urls in the first page, and total number of pages
    lst_url_all, n_total = find_lst_paper(url_section_first_page, get_total=True)
    # lst_url_all = find_lst_paper(url_section_first_page, get_total=False)

    if int(n_total) > 1:
        # find all url for pages
        lst_url_section = []
        for i in range(2, int(n_total) + 1):
            # get url for every page
            url_section_one_page = base_url + "?npage={}".format(i) + base_url1 + "&Network=no&lim=false"
            # save url
            lst_url_section.append(url_section_one_page)

        print("-"*80)
        print(f"start getting url for every page in {name_section}")
        print(f"total pages: {n_total}")
        start_time = time.perf_counter()

        try:
            count_faliure = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
                futures = []
                for url in lst_url_section:
                    futures.append(executor.submit(find_lst_paper, url))
                for future in tqdm(concurrent.futures.as_completed(futures), total=int(n_total)-1):
                    if not future.result():
                        count_faliure += 1
                        print(future.result())
                        print("count_faliure：", count_faliure)
                        if count_faliure > 10:
                            print("count_faliure larger than 10, the rest is not working, we exit")
                            executor.shutdown(wait=False)
                            # executor._threads.clear()
                            # concurrent.futures.thread._threads_queues.clear()

        except Exception as er:
            print(er)

        # combine all url in one topic
        # store lst_url that dont_work
        # store lst_url that work
        lst_url_dont_work = []
        for i in range(len(futures)):
            future = futures[i]
            # if future.exception() is not None:
            if not futures[i].result():
                lst_url_dont_work.append(lst_url_section[i])
            else:
                lst_url_all = lst_url_all + future.result()
    else:
        lst_url_dont_work = []

    # put it in set to drop duplicates
    lst_url_all = set(lst_url_all)

    print(f"finish getting url for every page in {name_section}")
    print(f"total pages: {n_total}")
    print(f"total futures: {len(futures)}")
    print(f"total_urls: {len(lst_url_all)}")
    print(f"used time: {round((time.perf_counter() - start_time)/60,1)} minutes")

    # save lst_title_url to text file
    with open(f'ssrn_url_lst_{name_section}.txt', 'w', newline='') as file:
        file.write("\n".join(lst_url_all))

    return lst_url_all, lst_url_dont_work


def get_all_paper_info_in_sections(lst_url_section, name_section):
    """
    1. get_all_paper_info_in_sections by using multiple threads
    2. for every 100 urls, we save the results
    3. rehandle urls that don't work

    :param lst_url_section:
    :param name_section:
    :return:
    """

    print("-" * 80)
    print(f"getting information for every url for section {name_section}")
    print(f"total length: {len(lst_url_section)}")

    j = 1
    lst_res_handle = []
    lst_res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for url in lst_url_section:
            futures.append(executor.submit(find_info_in_one_paper, url))
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(lst_url_section)):
            results = future.result()
            lst_res.append(results)

            # if the results dont work
            if (results[1] == ",") & (results[2] == ","):
                lst_res_handle.append(results)

            # for every 100 urls, we save the results
            if j % 100 == 0:
                # write information into csv file
                print("-" * 80)
                print("writing information into csv file")
                with open(os.path.join(os.getcwd(), name_section, f'ssrn_info_{j}.csv'), 'w+', encoding="utf-8",
                          newline="") as file:
                    write = csv.writer(file)
                    write.writerows(lst_res)

                # reset list
                lst_res = []

            # move next
            j += 1

    print(f"finish getting url for every url in {name_section}")
    print(f"total length: {len(lst_url_section)}")
    print(f"total futures: {len(futures)}")
    print(f"total urls that dont work: {len(lst_res_handle)}")

    # rehandle the urls that dont work -- second time
    lst_res = []
    lst_res_handle_2 = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for url in lst_res_handle:
            futures.append(executor.submit(find_info_in_one_paper, url[0]))
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(lst_res_handle)):
            results = future.result()
            lst_res.append(results)

            # if the results dont work
            if (results[1] == ",") & (results[2] == ","):
                lst_res_handle_2.append(results)

    print("-" * 80)
    print("writing information into csv file")
    with open(os.path.join(os.getcwd(), name_section, f'ssrn_info_rehandle.csv'), 'w+', encoding="utf-8",
              newline="") as file:
        write = csv.writer(file)
        write.writerows(lst_res)

    print(f"after 2 handle, {len(lst_res_handle_2)} urls still dont work")

    return lst_res_handle_2


def replace_all(text, dic):
    """
    replace multiple substrings to a string
    :param text:
    :param dic:
    :return:
    """
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


if __name__ == "__main__":

    # url for financial economic
    url_topic = "https://www.ssrn.com/index.cfm/en/fen/"

    # get the url and the name for every topic
    # return into list
    lst_url = get_link_for_all_section_in_one_topic(url_topic)

    # string replacement dic
    dic = {
        "&amp": "",
        ";": "",
        ":": "",
        "'": "",
        "(": "",
        ")": "",
        ",": " ",
        " ": "_"
    }

    # dictionary to store urls for every section
    dic_section_urls = {}

    # we scrape all the paper in every topic in in financial economic
    for i in range(1, len(lst_url)):
        url_section = lst_url[i]["url"].replace("&amp;", "&")
        name_section = replace_all(lst_url[i]["name"], dic)
        print(name_section, url_section)

        # get all urls in one section, save them in txt file
        lst_url_section, lst_url_section_dont_work = find_all_urls_in_section(url_section, name_section)

        if not lst_url_section_dont_work:
            print('-'*80)
            print("dont work ")
            print(lst_url_section_dont_work)

        # store the list in dictionary
        dic_section_urls[name_section] = lst_url_section

        time.sleep(1)

    # then we scrape all info for all urls in one section
    for i in range(len(lst_url)):
        # get the section name
        name_section = replace_all(lst_url[i]["name"], dic)

        # create directory for to save all information
        if not os.path.exists(name_section):
            os.makedirs(name_section)

        # read txt file as list
        # load urls in one section
        lst_url_section = open(os.path.join(os.getcwd(), "ssrn_url_list", f"ssrn_url_lst_{name_section}.txt")).read().split("\n")

        # get information for every url
        get_all_paper_info_in_sections(lst_url_section, name_section)


