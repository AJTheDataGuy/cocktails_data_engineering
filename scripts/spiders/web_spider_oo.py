"""Reusable web spider to scrape
text and PDF data from websites

If indexing is turned on (when
instantiating the instance) the spider
will uploads the following to SQL:
1. Parent-child link relationships
2. List of unique links

Notes:
1. This spider currently only supports
saving files to a local drive. I do intend
to add support for other options such as
saving to Azure at another date.

2. Indexing is turned on an off by providing
a helper object (of the SpiderIndexSQLSetup class)
which defines SQL locations for indexing. If not provided
indexing will be turned off by default.

TO DO:
1. Add more error handling for the request
2. Run black and pylint
"""
# Standard Library Imports
import hashlib
from itertools import product
from random import randint
from time import sleep

# 3rd Party Imports
from bs4 import BeautifulSoup
import pandas as pd
import requests

# Custom Module Imports
from spider_indexing_dataclass import SpiderIndexSQLSetup

class WebSpider:
    """Class to scrape text and PDF data from websites"""
    def __init__(self,
                 root_site:str,
                 pen_depth:int,
                 raw_files_save_path:str,
                 indexing_definitions_obj:SpiderIndexSQLSetup=None
                 ):
        """Parameters are defined as follows:

        1. Root site: The homepage / domain of the website to scrape

        2. pen_depth: Penetration depth for how many layers of links
        to dive into the website. Root starts at depth of 1.

        3. raw_files_save_path: Local directory for where to save
        PDF and text files from the web scraping. For now, only
        local directories are supported. In the future I intend
        to add support for Azure.

        4. indexing_definitions_obj: Special helper class object
        to define where child-parent link relationships and the list
        of unique links should be saved in SQL. Code to instantiate this
        class is included in this package. If not provided at instantiation,
        indexing will be turned off by default.
        """
        self.root_site = root_site
        self.pen_depth = pen_depth
        self.raw_files_save_path = raw_files_save_path

        if indexing_definitions_obj is not None:
            try:
                assert isinstance(indexing_definitions_obj,SpiderIndexSQLSetup)
            except AssertionError as ae:
                raise ValueError("Error: indexing definitions must\
                              be part of a SpiderIndexSQLSetup class instance") from ae
            self.indexing_definitions_obj = indexing_definitions_obj
            self.indexing_on = True
        else:
            self.indexing_on = False

        self.unique_links_set = set()
        self.bind_session_with_header()
        self.bind_filter_word_list()
        

    def run_spider(self):
        """Runs the web spider. Main routine for the class."""
        for depth in range(self.pen_depth):
            parent_level_links = {self.root_site} if depth == 0 else child_level_links
            child_level_links = set()
            for parent_link in parent_level_links:
                # Only capture each web page once
                if parent_link in self.unique_links_set:
                    continue
                self.unique_links_set.add(parent_link)

                site_request = self.get_request_with_delay(parent_link)
                if site_request.status_code != 200:
                    continue

                # Save the raw file - text or PDF
                if parent_link.lower().endswith('pdf'):
                    self.save_webpage_as_pdf(parent_link,site_request)
                elif parent_link.lower().endswith('xlsx'):
                    continue
                elif parent_link.lower().endswith('xls'):
                    continue
                else:
                    self.save_webpage_as_text(parent_link,site_request)

                # Transform - find and clean the links to keep the spider out of trouble
                raw_child_links = self.get_all_links_from_page(site_request)
                cleaned_child_links = self.clean_webpage_links(raw_child_links)
                child_level_links.update(cleaned_child_links)

                if self.indexing_on:
                    # Record parent child link relationships
                    parent_child_df = self.create_parent_child_dataframe(parent_link, 
                                                                     child_level_links)
                    self.upload_data_to_sql(parent_child_df,flag="index")

        if self.indexing_on:
            # Upload final list of unique links to sql
            # As a dataframe
            unique_links_df = self.create_unique_links_df()
            self.upload_data_to_sql(unique_links_df,flag="unique_links")
        self.indexing_definitions_obj.sql_engine.dispose()


    def bind_session_with_header(self) -> requests.Session:
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
        self.session = session

    def bind_filter_word_list(self):
        """Defines a list of substrings
        used to clean the links from each webpage.
        Tries to keep the spider out of trouble.

        Can potentially be defined in another way in the future.
        As this list should be fairly standard I have decided to leave
        it defined here for now.

        Used to remove links such as:

        1. Careers (keeps the spider from looking like it is applying for jobs. How
        embarrassing would it be if the spider got a better job than me?)
        2. Contact Us pages (keeps the spider from accidently contacting anyone)
        3. Login pages (keeps the spider from being detected - login pages will
        require a post and not a get)
        4. Any other https listed pages
        5. Bill payment pages
        6. Values of None
        7. Any PDFs (tells the webspider not to download the PDFs)
        8. Facebook, Instagram, etc.
        """

        filter_words_list = [
            "career",
            "login",
            "pay",
            "your",
            "account",
            "auth",
            "contact",
            "activate",
            "reservation",
            "book",
            'tel',
            'facebook',
            'instagram',
            'subscribe',
            'google',
            'linkedin',
            'youtube',
            'mail',
            'app',
            'App'
        ]
        self.filter_word_list = filter_words_list

    def get_request_with_delay(
        self,
        parent_link,
        min_delay_s=40,
        max_delay_s=90,
        allow_redirects=False,
        timeout=20,
    ):
        """Sends a get request to a webpage with a sleep
        request before executing so it doesn't overload
        the web server

        Arguments for min_delay, max_delay etc. could hypothetically
        be set at the instance instantiation level - something to
        perhaps look at in the future but they seem fine here for now.

        Returns the request
        """
        sleep(randint(min_delay_s, max_delay_s))
        return self.session.get(parent_link, allow_redirects=allow_redirects, timeout=timeout)
    
    @staticmethod
    def get_all_links_from_page(returned_request, parse_mode:str="html.parser") -> list:
        """Returns all the <a> tag links from a single webpage
        as a list of links
        """
        links_list = []
        soup = BeautifulSoup(returned_request.content, parse_mode)
        links = soup.find_all("a")
        for link in links:
            links_list.append(link.get("href"))
        return links_list


    def clean_webpage_links(self,links_list: list) -> set:
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
        for link_tuples in product(links_list, self.filter_word_list):
            if (link_tuples[1] not in link_tuples[0]) and (
                link_tuples[0] not in (None, "/")
            ):
                clean_links.append(link_tuples[0])
        return set(clean_links)

    def create_parent_child_dataframe(
        self, parent_link: str, child_links: set
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
        tuples_list = list(product([self.pen_depth], [parent_link], child_links))
        return pd.DataFrame(tuples_list, columns=["pen_depth", "parent_link", "child_link"])


    def create_unique_links_df(self) -> pd.DataFrame:
        """Converts the list of unique links
        to a dataframe that can be uploaded to SQL
        """
        unique_link_tuples_list = list(enumerate(self.unique_links_set))
        return pd.DataFrame(unique_link_tuples_list, columns=["link_id", "link_name"])


    def upload_data_to_sql(
        self,
        data_df: pd.DataFrame,
        flag:str
    ):
        """Only applies if indexing is turned on.
        Uploads data for child-parent link relationships
        and a list of unique links into SQL.
        """
        if flag == "index":
            table_name = self.indexing_definitions_obj.db_index_table_name
            schema = self.indexing_definitions_obj.db_index_schema

        elif flag == "unique_links":
            table_name = self.indexing_definitions_obj.unique_links_table_name
            schema = self.indexing_definitions_obj.unique_links_schema
        else:
            raise ValueError("Error: flag must be either 'index' or 'unique_links'")
        
        data_df.to_sql(
            name=table_name,
            con=self.indexing_definitions_obj.sql_engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi"
        )

    def save_webpage_as_text(self,web_url:str,returned_request):
        """Saves webpage text locally as a .txt file"""
        save_name = self.generate_output_file_name(web_url,file_type_flag="txt")
        with open(self.raw_files_save_path+save_name+'.txt','a') as file:
            file.write(f"root_site:{self.root_site}"+"\n")
            file.write(f"web_url:{web_url}"+"\n")
            file.write(returned_request.text)
    
    def save_webpage_as_pdf(self,web_url:str,returned_request):
        """Saves webpage pdf files locally as a .pdf file"""
        save_name = self.generate_output_file_name(web_url,file_type_flag="pdf")
        with open(self.raw_files_save_path+save_name+'.pdf','wb') as file:
            file.write(returned_request.content)

    
    def generate_output_file_name(self,webpage:str,file_type_flag:str)->str:
        """Gives each webpage a unique file name for saving locally
        Returns the unique name
        """
        try:
            assert file_type_flag.lower() in ('pdf','txt')
        except AssertionError as ae:
            raise ValueError("Error: flag must be pdf or text") from ae
        cleaned_root = self.root_site.replace("\\","_").replace("/","_")
        unique_url_hash = hashlib.md5((self.root_site+webpage).encode()).hexdigest()
        output_name = "".join([cleaned_root,
                               unique_url_hash,
                               ".",
                               file_type_flag.lower()])
        return output_name






