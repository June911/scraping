"""
scraping from ssrn
"""

# import packages
import csv
import time
import requests
from bs4 import BeautifulSoup
from ordered_set import OrderedSet
from tqdm import tqdm


# send request and return soup
def quickSoup(url):
    try:
        header = {}
        header['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
        soup = BeautifulSoup(requests.get(url, headers=header, timeout=10).content, 'html.parser')
        return soup
    except Exception:
        return None


# find relevent info in the web
def find_info(body, text_list):
    # find title
    title = body.find('h1').get_text().replace("\n", "")

    # find author
    authors = text_list[0].replace(title, "").replace(" :: SSRN", "").replace(" by ", "").replace(", ", ":")

    # find abstract
    # need to add quotation mark for conveniently storing in csv
    abstract = "\"{}\"".format(body.find(class_="abstract-text").get_text().replace("\n", "").replace("Abstract", ""))

    # find journal
    journal = "\"{}\"".format(body.find(class_="reference-info").get_text().replace("\n", ""))

    # find year
    date = body.find(class_="note note-list").get_text().replace("\n", "")
    if "Last revised: " in date:
        date = date.split("Last revised: ")[1]
    elif "Posted: " in date:
        date = date.split("Posted: ")[1]
    else:
        date = ""
        print("no date, need to handle")

    # find university
    universities = body.find(class_="authors authors-full-width").find_all("p")
    universities = [university.get_text() for university in universities]
    # convert list to string with ";" as seperator
    universities = "\"{}\"".format(",".join(universities))

    results = [title, abstract, authors, journal, date, universities]
    return results


def scrape_info(url):

    # request url and parse the html
    # get the article
    soup = quickSoup(url)

    if soup:
        # the main body
        body = soup.find(class_="container abstract-body")

        # the list of text
        text_list = OrderedSet(soup.get_text().split("\n")) - {''}

        # get web info
        results = find_info(body, text_list)

        # add url in the beginning
        # results["url"] = url
        results = [url] + results

        # return ",".join(results)
        return results

    else:
        print("cannot request the url")

        return ["no results", ""]


# find the urls of all papers in one url  in one topic
def find_lst_paper(url_topic, get_total=False):

    # request url and parse the html
    # get the web
    soup = quickSoup(url_topic)

    if soup:
        # find the body that contains all url
        body = soup.find(class_="tbody")

        # find the urls
        title_url = body.find_all(class_="title optClickTitle", href=True)

        # transform into list
        lst_title_url = [i["href"] for i in title_url]

        # get total number of pages
        if get_total:
            n_total = soup.find(class_="results-header").find(class_="total").get_text()
            return lst_title_url, n_total
        else:
            return lst_title_url

    else:
        print("cannot request the url")


# find all paper in one topic and store papers' info into a csv
def find_topic_info(n_total):
    # start loop for all pages
    # starting at 1, ending at n_total
    lst_url_all = []

    print("-"* 80)
    print("getting url for every page")

    for i in range(1, int(n_total) + 1):
        # get url for every page
        url_topic = "https://papers.ssrn.com/sol3/JELJOUR_Results.cfm?npage={}&form_name=journalBrowse&journal_id=1175282&Network=no&lim=false".format(
            i)

        # find all paper urls for every page
        lst_title_url = find_lst_paper(url_topic, get_total=False)

        # append to the lst_all
        lst_url_all = lst_url_all + lst_title_url

        print(i, len(lst_url_all))
        time.sleep(0.5)

    # get information for every url
    print("-"* 80)
    print("getting information for every url")
    lst_res = []
    for url in tqdm(lst_url_all):
        results = scrape_info(url)
        lst_res.append(results)
        time.sleep(0.5)

    # write information into csv file
    file = open('ssrn_info.csv', 'w+', newline='')
    with file:
        write = csv.writer(file)
        write.writerows(lst_res)


if __name__ == "__main__":

    # test scraping from one paper
    url = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id={}".format(str(2198490))
    results = scrape_info(url)
    print(results)

    # test getting list of urls in one topic
    url_topic = "https://papers.ssrn.com/sol3/JELJOUR_Results.cfm?npage={}&form_name=journalBrowse&journal_id=1175282&Network=no&lim=false".format(1)
    lst_title_url, n_total = find_lst_paper(url_topic, get_total=True)
    print(lst_title_url[:5])
    print(n_total)

    # test getting all paper info in one topic
    find_topic_info(n_total)