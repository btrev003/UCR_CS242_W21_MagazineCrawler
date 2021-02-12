import scrapy
import random
from image_spider.items import ImageItem
from scrapy.linkextractors import LinkExtractor

def uniqueID():
    seed = random.randint(1, 9999999)
    while True:
        yield seed
        seed += 1

class ImageSpider(scrapy.Spider):
    name = "ImageSpider"
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': 10000,
    }
    start_urls = ['http://www.coverbrowser.com/']
  
    def parse(self, response):
        for selector in response.css('p.navigation a::attr(href)'):
            if(selector.get()[0] == '/'):
                yield response.follow(response.urljoin(selector.get()), callback=self.parse_magazine)
    
    def parse_magazine(self, response):
        unique_id = uniqueID()
        for img in response.css('img[alt]'):
            image_item = ImageItem()
            image_item['uid'] = next(unique_id)
            image_item['title'] = img.attrib['alt']
            image_item['image_urls'] = [response.urljoin(img.attrib['src'])]
            yield image_item
        
        next_page = response.css('p.issuesNavigationBottom a::attr(href)').getall()[-1]
        yield response.follow(next_page, callback=self.parse_magazine)