"""Get top videos from a query, opening up to the 49th page of results."""

import json
import ssl
import urllib.request, urllib.parse, urllib.error
import time
import os

KEY = 'INSERT YOUR API KEY HERE'
ENDPOINT = 'https://www.googleapis.com/youtube/v3/search?'
SAVEPATH = 'INSERT HERE THE SAVE PATH FOR INCOMING TEXT FILE; PREFERABLY IN THE SAME PROJECT FOLDER'
FILE_FOR_SEARCHES = 'Search_data.txt'

def get_url_ctx():
     """Ignore SSL Certificate errors"""
     ctx = ssl.create_default_context()
     ctx.check_hostname = False
     ctx.verify_mode = ssl.CERT_NONE
     return ctx

def set_query(prompt):
     """Set and format search query."""
     search_query = input(prompt)
     return search_query
     
def set_url_parameters(query,pagetoken):
     """Set url parameters."""
     parms = dict()
     parms['key'] = KEY
     parms['part'] = "snippet"
     parms['q'] = query
     parms['type'] = 'video'
     parms['maxResults'] = '50'

     if pagetoken is not None:
          parms['pageToken'] = pagetoken

     return parms

def get_search_results(parms, ctx):
     """Get search result for one query."""
     url = ENDPOINT + urllib.parse.urlencode(parms)
     print(f'Getting URL: {url}')
     search_results = urllib.request.urlopen(url,context=ctx)
     search_results_data = search_results.read().decode()
     return search_results_data

def write_to_file(search_results):
     """Write a file containing search results, and append succeeding results."""
     outfile = open(os.path.join(SAVEPATH,FILE_FOR_SEARCHES),'a',encoding = 'utf-8')
     outfile.write(search_results+"\n")
     outfile.close()

def get_next_page_token(search_results):
     """Get next page token to go to next pages of search result."""
     next_page_token = None
     results_json = json.loads(search_results)

     try:
          next_page_token = results_json['nextPageToken']
     except:
          next_page_token = None
     
     return next_page_token

def main():
     """Initialize parameters, loop over a series of searches
     by getting search results, writing them, and moving to the next page."""

     ctx = get_url_ctx()
     query = set_query('Insert search query: ')
     searches = 0
     search_results = None
     next_page_token = None
     
     #The next line is to limit each run of all files to one core search. 
     try:
          os.remove(os.path.join(SAVEPATH,FILE_FOR_SEARCHES))
     except:
          pass
     
     is_end_of_pages = False

     while searches < 50 and not is_end_of_pages:
          parms = set_url_parameters(query,next_page_token)
          searches += 1
          print(f'Getting search #{searches}')
          search_results = get_search_results(parms,ctx)
          write_to_file(search_results)
          next_page_token = get_next_page_token(search_results)
          if next_page_token == None:
               is_end_of_pages = True
          time.sleep(5)

main()
