import argparse
import asyncio
import logging
import os
import sys
import time
from collections import namedtuple
from concurrent.futures import CancelledError
from urllib.error import HTTPError
from urllib.request import urlopen

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError

from parser_with_xpath import NaiveHTMLParser

BASE_URL = 'https://news.ycombinator.com/'
BYTE_EXTENSIONS = ['pdf', 'jpg', 'png', 'gif']
OUTPUT_PATH = 'output/'
STATE_FILE_PATH = os.path.join(OUTPUT_PATH, 'crawled_urls.txt')
TIMEOUT = 15
SLEEP_TIME = 300


class Url(namedtuple('Url', ('url', 'path', 'number'))):
    def __eq__(self, other):
        return self.url == other.url


def get_parser_root(page):
    parser = NaiveHTMLParser()
    root = parser.feed(page)
    parser.close()
    return root


def get_current_urls(output_path):
    news_urls, comment_urls = [], []
    if os.path.isfile(output_path):
        with open(output_path, 'r') as f:
            for line in f:
                line = line.split()
                news_urls.append(line[0])
                comment_urls.append(line[1])
    return news_urls, comment_urls


def save_current_urls(output_path, news_urls, comment_urls):
    open_mode = 'a' if os.path.exists(output_path) else 'w'
    with open(output_path, open_mode) as f:
        for i, news_url in enumerate(news_urls):
            f.write('{0} {1}\n'.format(news_url, comment_urls[i]))


def get_and_create_paths(urls, output_path):
    paths = []
    for url in urls:
        path = url.replace('/', '_')
        os.makedirs(os.path.join(output_path, path))
        paths.append(path)
    return paths


def parse_news_urls(root, news_urls, base_url):
    new_news_urls = []
    story_elements = root.findall('.//a[@class="storylink"]')
    for story_element in story_elements:
        news_url = story_element.get('href')
        if news_url.startswith('/item?') or news_url.startswith('item?'):
            news_url = base_url + news_url
        if news_url not in news_urls:
            new_news_urls.append(news_url)
    return new_news_urls


def parse_comments_urls(root, comment_urls, base_url):
    new_comments_urls = []
    comment_elements = root.findall('.//td[@class="subtext"]')
    for comment_element in comment_elements:
        comment_url = base_url + comment_element[-1].get('href')
        if comment_url not in comment_urls:
            new_comments_urls.append(comment_url)
    return new_comments_urls


def parse_urls_in_comment(html, path):
    root = get_parser_root(html)
    elements = root.findall('.//a[@rel="nofollow"]')
    inner_urls, _urls_names = [], []
    for i, el in enumerate(elements):
        inner_url = Url(el.get('href'), path, i)
        if inner_url.url != 'bookmarklet.html' and inner_url not in inner_urls:
            inner_urls.append(inner_url)
    return inner_urls


async def get_comments_pages(url, session, path):
    async with session.get(url) as resp:
        html = await resp.text()
    return html, path, url


async def save_file(url, session, path, options, byte_extensions, comment_id='',):
    if comment_id:
        filename = comment_id + url.replace('/', '_')
    else:
        filename = url.replace('/', '_')
    file_path = os.path.join(options.output, path, filename)
    try:
        async with session.get(url) as resp:
            ext = url.split('.')[-1]
            if ext in byte_extensions:
                content = await resp.read()
            else:
                content = await resp.text()
            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, save_file_executor, file_path, content, ext, byte_extensions)
    except (HTTPError, ClientConnectorError, ValueError) as e:
        logging.error('error in download ' + url)
        logging.debug(e)
    except CancelledError:
        logging.debug('cancel in download ' + url)


def save_file_executor(filename, content, ext, byte_extensions):
    if ext in byte_extensions:
        with open(filename, 'wb') as f:
            f.write(content)
    else:
        if ext != 'html':
            filename += '.html'
        with open(filename, 'w') as f:
            f.write(content)


async def crawling_news(news_urls, paths, options, byte_extensions):
    async with aiohttp.ClientSession() as session:
        tasks = [save_file(service, session, paths[i], options, byte_extensions) for i, service in enumerate(news_urls)]
        done, pending = await asyncio.wait(tasks, timeout=options.timeout)
        for future in pending:
            future.cancel()


async def crawling_comments(comment_urls, paths, options, byte_extensions):
    async with aiohttp.ClientSession() as session:
        urls = []
        for i in range(0, len(comment_urls), 6):
            tasks = [get_comments_pages(service, session, paths[j]) for j, service in enumerate(comment_urls[i:i+6], i)]
            done, pending = await asyncio.wait(tasks, timeout=options.timeout)
            for future in done:
                html, path, url = future.result()
                inner_urls = parse_urls_in_comment(html, path)
                urls.extend(inner_urls)
        tasks = [save_file(url.url, session, url.path, options, byte_extensions, str(url.number)+'_') for url in urls]
        done, pending = await asyncio.wait(tasks, timeout=options.timeout)
        for future in pending:
            future.cancel()


def main(options, base_url, byte_extensions):
    while True:
        logging.info('Search urls')
        base_page = urlopen(base_url)
        root = get_parser_root(str(base_page.read()))

        news_urls, comment_urls = get_current_urls(options.state)
        new_news_urls = parse_news_urls(root, news_urls, base_url)
        news_urls.extend(new_news_urls)
        new_comment_urls = parse_comments_urls(root, comment_urls, base_url)
        comment_urls.extend(new_comment_urls)

        paths = get_and_create_paths(new_news_urls, options.output)
        if new_news_urls:
            logging.info('Start parse')
            start_time = time.time()
            ioloop = asyncio.get_event_loop()
            tasks = [
                ioloop.create_task(crawling_news(new_news_urls, paths, options, byte_extensions)),
                ioloop.create_task(crawling_comments(new_comment_urls, paths, options, byte_extensions))
            ]
            wait_tasks = asyncio.wait(tasks)
            ioloop.run_until_complete(wait_tasks)
            ioloop.close()
            logging.info('Parse end at {0:.1f} seconds'.format(time.time()-start_time))
        else:
            logging.info('Don\'t have new pages')

        save_current_urls(options.state, new_news_urls, new_comment_urls)
        logging.info('Next Search in {0} seconds'.format(options.sleep_time))
        time.sleep(options.sleep_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-st", "--state", default=STATE_FILE_PATH, help="state file path")
    parser.add_argument("-sl", "--sleep_time", default=SLEEP_TIME, help="time for before next parse", type=int)
    parser.add_argument("-o", "--output", default=OUTPUT_PATH, help="output path")
    parser.add_argument("-t", "--timeout", default=TIMEOUT, help="time out for async tasks", type=int)
    parser.add_argument("-l", "--log", default=None,  help='log file path')
    parser.add_argument("-d", "--debug", default=False,  help='debug logging', action="store_true")
    opts = parser.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.debug else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    try:
        main(opts, BASE_URL, BYTE_EXTENSIONS)
    except KeyboardInterrupt:
        logging.info('Program exit')
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
