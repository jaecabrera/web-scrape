import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass

import pandas as pd
import toml
from requests.exceptions import HTTPError
from pandas.errors import EmptyDataError
from requests_html import HTMLSession
import asyncio

DATA_FILE_NAME = "pubmed_data.csv"


@dataclass(slots=True)
class PubEntry:
    """ A dataclass with attributes containing the entries scrapped in each url request (pubmed website)."""
    bold_text: str
    content_text: str


def get_params() -> dict:
    """
    Read `request.toml` from the parent directory.
    :return: A dict of parameters containing the `urls` and the `css selector`.
    :rtype dict:
    """
    try:

        with open('request.toml', 'r') as f:
            _p = toml.load(f)
            params = _p['scrape-params']

    except FileNotFoundError as e:
        print(e)
        sys.exit(3)

    finally:
        r_code = params['research_code']
        _url = params['url']
        request_urls = [_url + code for code in r_code]
        p = {'url': request_urls, 'selector': params['selector']}
        f.close()
        return p


@contextmanager
def html_session(request_url) -> None:
    """
    HTMLSession() class wrapped around a context manager.
    :param request_url: Contains the url address/'s for our requests.
    :return: None.
    :rtype None:
    """
    session = HTMLSession()
    try:
        response = session.get(request_url)
        if response.status_code != '404':
            yield response
        else:
            raise HTTPError
    except HTTPError as e:
        print(e)
        sys.exit(3)
    finally:
        session.close()


async def parse_pub_entry_data(response, selector) -> list[PubEntry]:
    """
    Parse the html from the url response (response) and selects (selector) the appropriate part of the text needed.
    For this use case we are finding the `Abstract` section:

    Example Text Data:

        Abstract
        [**Conclusion:**] ["All related content..."]

    :param response: contains the response content of the url
    :param selector: css selector that targets the specific text content we need.
    :return: A list of PubEntry class object.
    :rtype list[PubEntry]:
    """
    abstract_content = response.html.find(selector)
    pub_entries = []

    for p in abstract_content:

        _b: str = ''
        for bold_text in p.find("strong"):
            _b = bold_text.text

        content = p.text.replace(_b, "").strip()
        entry = PubEntry(_b, content)
        pub_entries.append(entry)

    return pub_entries


def make_data_dir_if_not_exists() -> None:
    """
    Creates the `output/pubmed_data.csv` directory & file if not exists
    :return: None
    :rtype None:
    """
    if not os.path.exists('output'):
        os.mkdir("output")
    if not os.path.exists(f'output/{DATA_FILE_NAME}'):
        with open(f'output/{DATA_FILE_NAME}', 'w') as _:
            pass


def insert_data(data: dict) -> None:
    """
    Inserts a data entry to `output/pubmed_data.csv`
    :param data:
    :return: None
    :rtype None:
    """
    new_data = pd.DataFrame.from_dict(data)
    try:
        pubmed_df = pd.read_csv(f'output/{DATA_FILE_NAME}')

    except EmptyDataError:
        pubmed_df = pd.DataFrame()

    pubmed_df0 = pd.concat([pubmed_df, new_data], ignore_index=True)
    pubmed_df0.to_csv(f'output/{DATA_FILE_NAME}', index=False)


async def main():

    # creates the folder and .csv file if it doesn't exist in the root directory
    make_data_dir_if_not_exists()

    # get the params needed such as urls and css selector
    p = get_params()

    # iterate of all the urls listed n the params
    for _url_code_requests in p['url']:

        # create html session context manager for handling response contents
        with html_session(_url_code_requests) as session_response:

            # parse the data with await to avoid data inconsistencies and errors
            pub_entry = await parse_pub_entry_data(session_response, p['selector'])
            data = {'url': _url_code_requests, 'text': []}

            # create a dictionary containing the url and the abstract's `sections` (nested dict. for multi-sections)
            for index, entries in enumerate(pub_entry):
                bold_text = entries.bold_text
                content_text = entries.content_text
                data['text'].append({f'section_{index}': {'bold_text': bold_text, 'content_text': content_text}})

            # insert the data into pubmed_data.csv
            insert_data(data)


if __name__ == '__main__':
    asyncio.run(main())
