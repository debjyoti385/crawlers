import scrapy
import json
from wiki.items import WikiItem
from scrapy.http import Request
from scrapy_splash import SplashRequest
from scrapy.http.headers import Headers
import time
# import dateutil.parser as dparser
#
# def parseDate( date_string):
#     return dparser.parse(date_string, fuzzy=True)

class WikiSpider(scrapy.Spider):
    name = 'wikipedia_search'
    def __init__(self, **kwargs):
        self.total = 500
        super().__init__(**kwargs)
        self.search_api_url = "https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json"
        try:
            self.keywords = self.search.split(',')
        except:
            print("Use '-a search=keyword1,keyword2' to search.")
            exit()

        for word in self.keywords:
            sroffset = 0
            total = int(self.total)
            if total > sroffset:
                srlimit = min(500, total-sroffset)
                self.start_urls.append((word, self.search_api_url + "&self.srlimit="+str(srlimit) + "&sroffset="+ str(sroffset) + "&srsearch=" + word))
                sroffset = sroffset + srlimit


    def start_requests(self):
        for word, url in self.start_urls:
            print ("INDEX URL : " + url)
            headers = Headers({'Content-Type': 'application/json'})
            # request = scrapy.Request(url, self.parse, headers=headers, meta={
            #     'splash': {
            #         'endpoint': 'render.html',
            #         'args': {'wait': 1.0}
            #     }
            # })
            request = scrapy.Request(url, self.parse, headers=headers)
            request.meta['word'] = word
            yield request


    def parse(self, response):
        print(response.body_as_unicode())
        extract_url = 'https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro&explaintext&exsectionformat=raw&format=json&exintro=&pageids='
        jsonresponse = json.loads(response.body_as_unicode())
        word = response.meta['word']
        url = extract_url
        pageids = [item['pageid'] for item in  jsonresponse['query']['search']]
        N=10
        chunk_pageids = [pageids[i * N:(i + 1) * N] for i in range((len(pageids) + N - 1) // N )]
        for ids in chunk_pageids:
            str_ids = '|'.join([str(id) for id in ids])
            url = extract_url + str_ids
            headers = Headers({'Content-Type': 'application/json'})
            request = scrapy.Request(url, callback = self.save, headers= headers)
            request.meta['word'] = word

            yield request


    def save(self,response):
        jsonresponse = json.loads(response.body_as_unicode())
        print(jsonresponse['query']['pages'])
        word = response.meta['word']
        pages = jsonresponse['query']['pages']
        for page in pages:
            item = WikiItem()
            item['page_id'] = page
            item['keyword'] = word
            try:
                item['title'] = pages[page]['title']
            except:
                print("NO TITLE")
                item['title'] = ''

            try:
                item['content'] = pages[page]['extract']
            except:
                print("NO CONTENT")
                item['content'] = ''

            yield item
