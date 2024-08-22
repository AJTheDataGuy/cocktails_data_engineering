"""Reusable web spider to index websites
and scrape text data

Takes the following parameters:
1. Root site. Base site to scrape.
2. Penetration depth. How many levels into
the website to scrape. This includes the root level.
So a penetration depth of 3 will scrape the root site
and two levels down.

Uploads the following to PostgreSQL:
1. Parent-child link relationships
2. List of unique links

TO DO:
1. ADD TEXT SAVING FRAMEWORK
2. Add handling for PDF file requests
"""
# Standard Library Imports
from itertools import product
from random import randint
from time import sleep

# 3rd Party Imports
from bs4 import BeautifulSoup
import pandas as pd
import requests

# Custom Module Imports
from scripts.db_connections.superseded import db_connection_funcs


def run_spider(
    root_site: str = r"https://www.alintaenergy.com.au/", pen_depth: int = 1
):
    """Main"""
    unique_links_set = set()
    postgres_engine = db_connection_funcs.create_postgresql_engine()
    session = get_session_with_header()
    filter_word_list = get_filter_word_list()

    for depth in range(pen_depth):
        parent_level_links = {root_site} if depth == 0 else child_level_links
        child_level_links = set()

        for parent_link in parent_level_links:
            if parent_link in unique_links_set:
                continue
            unique_links_set.add(parent_link)

            site_request = get_request_with_delay(session, parent_link)
            if site_request.status_code != 200:
                continue

            raw_child_links = get_all_links_from_page(site_request)
            cleaned_child_links = clean_webpage_links(raw_child_links, filter_word_list)
            child_level_links.update(cleaned_child_links)

            parent_child_df = create_parent_child_dataframe(
                depth, parent_link, child_level_links
            )
            upload_data_to_postgresql(
                parent_child_df,
                postgres_engine,
                table_name="parent_child_relationships",
            )

    unique_links_df = create_unique_links_df(unique_links_set)
    upload_data_to_postgresql(
        unique_links_df,
        postgres_engine,
        table_name="unique_links",
        exists_mode="append",
    )
    postgres_engine.dispose()


def get_session_with_header() -> requests.Session:
    """Returns a session object with a predefined
    user agent header. In the future authentication can
    be added as well.
    """
    agent = "".join(["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) ",
                    "AppleWebKit/601.3.9 (KHTML, like Gecko) ",
                    "Version/9.0.2 Safari/601.3.9"])
    session = requests.Session()
    headers = {
        "User-Agent": f"{agent}"
    }
    session.headers.update(headers)
    return session


def get_all_links_from_page(returned_request, parse_mode="html.parser") -> list:
    """Returns all the <a> tag links from a single webpage
    as a list of links
    """
    links_list = []
    soup = BeautifulSoup(returned_request.content, parse_mode)
    links = soup.find_all("a")
    for link in links:
        links_list.append(link.get("href"))
    return links_list


def clean_webpage_links(links_list: list, filter_word_list: list) -> set:
    """Cleans the list of links from a single webpage
    to help keep the web spider out of trouble

    May be a good idea to update when moving to other websites.

    Check 1: Removes links to the following pages:

    1. Careers (keeps the spider from looking like it is applying for jobs)
    2. Contact Us pages (keeps the spider from accidently contacting anyone)
    3. Login pages (keeps the spider from being detected - login pages will
    require a post and not a get)
    4. Any other https listed pages
    5. Bill payment pages
    6. Values of None
    7. Any PDFs (tells the webspider not to download the PDFs)

    Check 2: Also cleans links that are None or the root site (represented by a slash /)

    Check 3: Removes redundant links in case a single webpage has multiple
    links to the same website

    Returns a cleaned set of unique links
    """
    clean_links = []
    for link_tuples in product(links_list, filter_word_list):
        if (link_tuples[1] not in link_tuples[0]) and (
            link_tuples[0] not in (None, "/")
        ):
            clean_links.append(link_tuples[0])
    return set(clean_links)


def get_filter_word_list():
    """Defines a list of substrings
    used to clean the links from each webpage

    Used to remove links to the following pages:

    1. Careers (keeps the spider from looking like it is applying for jobs)
    2. Contact Us pages (keeps the spider from accidently contacting anyone)
    3. Login pages (keeps the spider from being detected - login pages will
    require a post and not a get)
    4. Any other https listed pages
    5. Bill payment pages
    6. Values of None
    7. Any PDFs (tells the webspider not to download the PDFs)
    """

    filter_words_list = [
        "career",
        "login",
        "http",
        "pay",
        "your",
        "account",
        "auth",
        "contact",
        "pdf",
        "activate",
    ]
    return filter_words_list


def create_parent_child_dataframe(
    depth: int, parent_link: str, child_links: set
) -> pd.DataFrame:
    """Creates a data structure (pandas dataframe)
    to record parent-child relationships and depth
    so the dataframe can be commited to SQL as the website
    is scraped

    First takes the depth, parent link, and list of child links and converts
    it into a list of tuples to get all the combinations. Uses the products
    function from the itertools library

    Returns a dataframe with all the parent-child relationships for the parent link
    """
    tuples_list = list(product([depth], [parent_link], child_links))
    return pd.DataFrame(tuples_list, columns=["pen_depth", "parent_link", "child_link"])


def create_unique_links_df(unique_links: set) -> pd.DataFrame:
    """Converts the list of unique links
    to a dataframe that can be uploaded to SQL
    """
    unique_link_tuples_list = list(enumerate(unique_links))
    return pd.DataFrame(unique_link_tuples_list, columns=["link_id", "link_name"])


def upload_data_to_postgresql(
    data_df: pd.DataFrame,
    postgres_engine,
    table_name: str,
    schema="public",
    exists_mode="append",
):
    """Uploads data to postgresql
    Exact postgreSQL engine is specified in the
    db_connection_functions helper file

    Parameterised so data can be uploaded in different ways.
    For example, parent-child dataframes can be uploaded in small chunks
    while the spider runs.
    """
    data_df.to_sql(
        name=table_name,
        con=postgres_engine,
        schema=schema,
        if_exists=exists_mode,
        index=False,
        method="multi",
    )


def get_request_with_delay(
    session,
    parent_link,
    min_delay_s=40,
    max_delay_s=90,
    allow_redirects=False,
    timeout=20,
):
    """Sends a get request to a webpage with a sleep
    request before executing so it doesn't overload
    the web server

    Returns the request
    """
    sleep(randint(min_delay_s, max_delay_s))
    return session.get(parent_link, allow_redirects=allow_redirects, timeout=timeout)

def write_file(response,azure_location,link_name:str):
    """Writes the webpage content to Azure Data Lake
    
    For a normal webpage, saves the webpage text as a txt file

    For a PDF, runs a procedure to save the full PDF file (which can
    then be further parsed, or handled directly by Azure Open AI
    )
    """
    if link_name.lower().endswith("pdf"):
        pass
    else:
        pass