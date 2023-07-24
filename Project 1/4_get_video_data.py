"""Get video data from the database of related videos and search results,
requested to the API in batches."""

import ssl
import urllib.request, urllib.parse, urllib.error
import time
import os
import sqlite3

KEY = 'INSERT KEY HERE'
ENDPOINT = 'https://www.googleapis.com/youtube/v3/videos?'
SAVEPATH = 'INSERT HERE THE SAVE PATH FOR INCOMING TEXT FILE; PREFERABLY IN THE SAME PROJECT FOLDER'
FILE_FOR_VIDEOS = 'Video_data.txt'
INTERVALS = 50

def get_url_ctx():
     """Ignore SSL Certificate errors"""
     ctx = ssl.create_default_context()
     ctx.check_hostname = False
     ctx.verify_mode = ssl.CERT_NONE
     return ctx

def set_url(id_concat):
     """Set url parameters, and append them to the endpoint."""
     parms = dict()
     parms['key'] = KEY
     parms['part'] = "snippet,statistics,topicDetails"
     parms['id'] = id_concat
     url = ENDPOINT + urllib.parse.urlencode(parms)
     return url

def create_video_list(query):
     """Create a list of videos from a table depending on the SQL query."""
     conn = sqlite3.connect('search_results.sqlite')
     cur = conn.cursor()
     video_rows = cur.execute(query)
     video_ids_list = []

     for row in video_rows:
         video_ids_list += [video for video in row if video not in video_ids_list]
     
     return video_ids_list

def creating_batches():
     """Create batches to be used as they save on queries consumed."""
     
     list_video_results = create_video_list('SELECT video_id FROM search_results')
     list_related_videos = create_video_list('SELECT video_id FROM related_searches')
     video_ids_list = set(list_video_results + list_related_videos)
     video_ids_list = list(video_ids_list)

     batches = []
     for i in range(0, len(video_ids_list),INTERVALS):
        batches.append(video_ids_list[i:i+INTERVALS])
    
     return batches

def write_to_file(data):
     """Write a file containing video data, and append succeeding results."""
     outfile = open(os.path.join(SAVEPATH,FILE_FOR_VIDEOS),'a',encoding = 'utf-8')
     outfile.write(data+"\n")
     outfile.close()

def write_video_data(ctx):
     """Send a batched video>list request."""
     batches = creating_batches()
     batch_count = 0

     for batch in batches:
        id_concat = ",".join(batch)
        url = set_url(id_concat)
        batch_count +=1
        print(f'Batch #{batch_count}. Getting URL: {url}')
        output = urllib.request.urlopen(url,context=ctx)
        video_data = output.read().decode()
        write_to_file(video_data)
        time.sleep(5)

def main():
     """Initialize parameters, loop over a series of searches
     by getting search results, writing them, and moving to the next page."""

     ctx = get_url_ctx()
     
     #The next line is to limit each run of all files to one core search. 
     try:
          os.remove(os.path.join(SAVEPATH,FILE_FOR_VIDEOS))
     except:
          pass
     write_video_data(ctx)

main()