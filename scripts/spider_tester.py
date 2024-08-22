"""Module to test the spider and ensure it runs as intended"""
from spiders.web_spider_oo import WebSpider
from db_connections.spider_indexing_dataclass import SpiderIndexSQLSetup

def main():
    """main"""
    test_spider_no_indexing3()

def test_spider_setup():
    """Test that the spider can be set up successfully"""
    sql_setup_obj = SpiderIndexSQLSetup()

    spider = WebSpider(
        root_site= r"http://www.picabar.com.au/",
        raw_files_save_path= r"~/Desktop/spider_files",
        pen_depth= 2,
        indexing_definitions_obj= sql_setup_obj
    )
    return spider

def test_spider_no_indexing():
    """Test the web spider on a simple website
    with no SQL indexing
    """
    spider = WebSpider(
        root_site= r"http://www.picabar.com.au/",
        raw_files_save_path= r"/home/thedudefish/Desktop/spider_files/",
        pen_depth= 2
    )
    spider.run_spider()

def test_spider_no_indexing2():
    """Test the web spider on a simple website
    with no SQL indexing
    """
    spider = WebSpider(
        root_site= r"https://theaviaryperth.com.au/",
        raw_files_save_path= r"/home/thedudefish/Desktop/spider_files/",
        pen_depth= 4
    )
    spider.run_spider()

def test_spider_no_indexing3():
    """Test the web spider on a simple website
    with no SQL indexing
    """
    spider = WebSpider(
        root_site= r"https://www.alintaenergy.com.au/",
        raw_files_save_path= r"/home/thedudefish/Desktop/spider_files/",
        pen_depth= 4
    )
    spider.run_spider()

def test_spider_no_indexing3():
    """Test the web spider on a simple website
    with no SQL indexing
    """
    sql_setup_obj = SpiderIndexSQLSetup()

    spider = WebSpider(
        root_site= r"https://www.alintaenergy.com.au/",
        raw_files_save_path= r"/home/thedudefish/Desktop/spider_files/",
        pen_depth= 1,
        indexing_definitions_obj=sql_setup_obj
    )
    spider.run_spider()


if __name__ == "__main__":
    main()
