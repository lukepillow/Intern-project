import scrapy
import json
import requests
from scrapy.http import TextResponse
import urllib2
import random
import time
import psycopg2
import gzip
import multiprocessing
import os
from StringIO import StringIO
from collections import defaultdict
import pandas as pd
%matplotlib inline
import random
import demjson
n = 20
date = '2017_07_16'

def apt_url(area,low,high,other = ''):
    return 'https://www.apartments.com/'+area+'/'+other +\
            str(low)+'-to-'+str(high)

def generate_url(low = 0,high = 10000,interval = 50, area = 'ca' ):
    while low + interval < high:
        yield apt_url(area,low,low + interval -1)
        low += interval 
    if low < high:
        yield apt_url(area,low,high)


def dynamic_range(low,high,area = 'ca',min_interval = 1):
    url = 'https://www.apartments.com/'+area+'/'+ \
                str(low)+'-to-'+str(low+min_interval)
    last_page = get_last_page(url)

    if last_page < 14:
        min_interval += 1
        

def connect_postgresql(
                       host='data-team.cfvkbgrgybof.us-west-1.rds.amazonaws.com',
                       user='power_user',
                       password='AEe5CroguvFMwYiw5bzRoQ'):
    """Set up the connection to postgresql database"""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ",e)
        
def close_save(cur,conn,all = 'False'):
    cur.close()
    try:
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Can not commit ',error)
    print('Closed cursor and saved the change')
    if all and conn is not None:
        conn.close()

def get_item(input_list,funtion = float):
    if len(input_list) > 0:
        try:
            return function(input_list[0])
        except:
            return str(input_list[0])
    else:
        return -1

def find_json(s):
    start = s.find('[')
    end = s.find(']')
    return s[start:end+1]

def find_next_json(s):
    n = 0
    found = 0
    for i in range(len(s)):
        if s[i] == '{' and found == 0:
            found = 1
            start = i
            n += 1
        elif s[i] == '{':
            n += 1
        elif s[i] == '}' and found == 1:
            n -= 1
            if n == 0:
                end = i
                try:
                    #print s[start:end+1]
                    data = json.loads(demjson.encode(demjson.decode(s[start:end+1])))
                    return data
                except:
                    print 'Could not load json data '
                    return -1
    print 'Could not find json data' 
    return -1
    
def get_listing_data(url):
    r = requests.get(url)
    response = TextResponse(r.url, body=r.text, encoding='utf-8')
    owner = get_item(response.xpath('//img[@class="logo"]/@alt').extract())
    title = get_item(response.xpath('//title/text()').extract())
    unit_type = str(' '.join(response.xpath('//span[@class="rentRollup"]/span[@class="shortText"]/text()').extract()))
    price_type = str(' '.join(''.join(response.xpath('//span[@class="rentRollup"]/text()').extract()).\
                              replace('\r','').replace('-','').replace('\n','').replace(u'\u2013','-').split()))
    street_address = str(get_item(response.xpath('//span[@itemprop="streetAddress"]/text()').extract()))
    city = get_item(response.xpath('//span[@itemprop="addressLocality"]/text()').extract())
    region = get_item(response.xpath('//span[@itemprop="addressRegion"]/text()').extract())
    zip_code = get_item(response.xpath('//span[@itemprop="postalCode"]/text()').extract())
    neighborhood = get_item(response.xpath('//div[@class="neighborhoodAddress"]/a/text()').extract())
    building_info = str('  '.join(response.xpath('//div[@class="specList propertyFeatures js-spec"]/ul/li/text()').extract()))
    n_of_unit_string = response.xpath('//div[@class="specList propertyFeatures js-spec"]/ul/li/text()').extract()
    l = response.xpath('//script[@type="text/javascript"]/text()').extract()[-1]
    image_json = find_json(l[l.find('carouselCollection'):])
    if len(n_of_unit_string) >= 2:
        try:
            n_of_unit = int(n_of_unit_string[1].split()[0])
        except:
            n_of_unit = -1
    else:
        n_of_unit =-1
    lat = get_item(response.xpath('//meta[@property="place:location:latitude"]/@content').extract())
    lon = get_item(response.xpath('//meta[@property="place:location:longitude"]/@content').extract())
    
    amenities = '\n'.join(response.xpath('//div[@class="specList js-spec"]/ul/li/text()').extract())
    
    json_data = find_next_json(l[l.find('startup.init'):])
    if json_data != -1:
        listingState = json_data['listingState']
        listingDescription = json_data['listingDescription']
        phoneNumber =json_data['phoneNumber']
        profileType = json_data['profileType']
    else:
        listingState = '-1'
        listingDescription = '-1'
        phoneNumber ='-1'
        profileType ='-1'
    
    data = (str(url),owner,title,unit_type,price_type,street_address,city,region,zip_code,\
            neighborhood,building_info,n_of_unit,lat,lon,image_json,amenities,listingState,listingDescription,\
            phoneNumber,profileType)
    return data        

def insert_data(url,db):
    insert = 'INSERT INTO crawled_apart_listing_{} VALUES('.format(date) + '%s,'*(n-1) + '%s);'
    try:
        data = get_listing_data(url)
        db.execute(insert, data)
    except Exception as e:
        if 'duplicate' not in str(e):
            print("Could not insert listing data, error is ",e)
    

    
def get_last_page(url):
    req = requests.get(url)
    response = TextResponse(req.url, body=req.text, encoding='utf-8')    
    l = response.xpath('//a[contains(@href,"https://www.apartments.com/")]/@data-page').extract()
    temp = []
    for i in l:
        try:
            page_number = int(str(i))
            temp.append(page_number)
        except:
            print "Not able to convert to int ",i
    if not temp:
        return 1
    return max(temp)

def best_interval(low,area,interval,high,min_step = 2 ,depth =1 ):
    if interval + low >= high:
        return high - low

    url = 'https://www.apartments.com/'+area+'/'+ \
                str(low)+'-to-'+str(low+interval)  
    last_page = get_last_page(url)
    if depth > 20 and last_page< 28:
        print "tried 20 times to adjust the interval"
        return interval
    if last_page <10:
        interval += min(50,max(1,int(interval *(random.random())/2)))
        #print 'interal too small, try bigger one ',interval,'number of pages is ',last_page
        depth += 1
        return best_interval(low,area,interval,high,min_step ,depth)
    elif last_page >= 28 and interval >min_step:
        interval -= max(int(interval*(random.random()))/3,1)
        #print 'interal too large, try smaller one ',interval,'number of pages is ',last_page
        depth += 1
        return best_interval(low,area,interval,high,min_step,depth)
    elif interval <= min_step and last_page == 28:
        print 'interval = {} still get 28 pages'.format(min_step)
        if url not in urls_need_further_process:
            urls_need_further_process.add(url)
        return min_step
        # best_interval = next_level(low,area)
    else:
        #print 'best interval',interval,'number of pages is ',last_page
        return interval

def run_spider_range(low=0,high = 800, area = 'ca',visited_urls =[] ):
    conn = connect_postgresql()
    db = conn.cursor()
    begin = time.time()
    for url in generate_url(low,high,area):
        start = time.time()

        last_page = get_last_page(url)
        url_list = [url + '/'+str(i+1) for i in range(last_page) ]
        print url_list[-1]

        for u in url_list:
            r = requests.get(u)
            response = TextResponse(r.url, body=r.text, encoding='utf-8')
            for listing in response.xpath('//article[contains(@data-listingid, "")]/@data-url').extract():
                if listing:
                    if listing in visited_urls:
                        continue
                    visited_urls.add(listing)
                    insert_data(listing,db)
                    conn.commit()

                else:
                    print 'No listing from url ',url
                    continue
        print 'Runs for {:4.3} miniuts'.format((time.time()-start)/60.0)       
    print 'Total time elapsed {:4.3} miniuts'.format((time.time()-begin)/60.0)
    return visited_urls
        
def run_spider(url,visited_urls =[]):
    conn,db = connect_postgresql()
    

    start = time.time()
    last_page = get_last_page(url)
    url_list = [url + '/'+str(i+1) for i in range(last_page) ]
    print url_list[-1]
    
    for u in url_list:
        r = requests.get(u)
        response = TextResponse(r.url, body=r.text, encoding='utf-8')
        for listing in response.xpath('//article[contains(@data-listingid, "")]/@data-url').extract():
            if listing:
                if listing in visited_urls:
                    continue
                visited_urls.add(listing)
                insert_data(listing,db)
                conn.commit()

            else:
                print 'No listing from url ',url
                continue
    print 'Runs for {:4.3} miniuts'.format((time.time()-start)/60.0)       
    return visited_urls