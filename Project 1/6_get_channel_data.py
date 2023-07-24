"""Get channels from video data table."""

import ssl
import urllib.request, urllib.parse, urllib.error
import time
import os
import sqlite3

KEY = 'INSERT YOUR API KEY HERE'
ENDPOINT = 'https://www.googleapis.com/youtube/v3/channels?'
SAVEPATH = 'INSERT HERE THE SAVE PATH FOR INCOMING TEXT FILE; PREFERABLY IN THE SAME PROJECT FOLDER'
FILE_FOR_CHANNELS = 'Channel_data.txt'
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
     parms['part'] = "snippet,brandingSettings,status,statistics,topicDetails"
     parms['id'] = id_concat
     url = ENDPOINT + urllib.parse.urlencode(parms)
     return url

def creating_batches():
     """Create batches to be used as they save on queries consumed."""
     conn = sqlite3.connect('search_results.sqlite')
     cur = conn.cursor()
     channel_rows = cur.execute('SELECT channel_id FROM video_data')
     channel_ids_list = []
     
     for row in channel_rows:
          channel_ids_list += [channel for channel in row if channel not in channel_ids_list]

     channel_ids_list = set(channel_ids_list)
     channel_ids_list = list(channel_ids_list)

     batches = []
     for i in range(0, len(channel_ids_list),INTERVALS):
          batches.append(channel_ids_list[i:i+INTERVALS])
    
     return batches

def write_to_file(data):
     """Write a file containing channel data, and append succeeding results."""
     outfile = open(os.path.join(SAVEPATH,FILE_FOR_CHANNELS),'a',encoding = 'utf-8')
     outfile.write(data+"\n")
     outfile.close()

def write_channel_data(ctx):
     """Send a batched channel>list request."""
     batches = creating_batches()
     batch_count = 0

     for batch in batches:
        id_concat = ",".join(batch)
        url = set_url(id_concat)
        batch_count +=1
        print(f'Batch #{batch_count}. Getting URL: {url}')
        output = urllib.request.urlopen(url,context=ctx)
        channel_data = output.read().decode()
        write_to_file(channel_data)
        time.sleep(5)

def main():
     """Initialize parameters, loop over a series of searches
     by getting search results, writing them, and moving to the next page."""

     ctx = get_url_ctx()
     
     #The next line is to limit each run of all files to one core search. 
     try:
          os.remove(os.path.join(SAVEPATH,FILE_FOR_CHANNELS))
     except:
          pass
     write_channel_data(ctx)

main()