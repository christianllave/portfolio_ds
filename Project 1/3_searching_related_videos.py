"""From the search results database,
get related videos of the first 10 videos
that have not gone through this process yet."""

import json
import ssl
import urllib.request, urllib.parse, urllib.error
import time
import os
import sqlite3

KEY = 'INSERT KEY HERE'
ENDPOINT = 'https://www.googleapis.com/youtube/v3/search?'
SAVEPATH = 'INSERT HERE THE SAVE PATH FOR INCOMING TEXT FILE; PREFERABLY IN THE SAME PROJECT FOLDER'
FILE_FOR_SEARCHES = 'Related_videos_'
LIMITER = 10

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
     
def set_url_parameters(query, bases):
    """Set url parameters. Limiting to first-page results only."""
    parms = dict()
    parms['key'] = KEY
    parms['part'] = "snippet"
    parms['q'] = query
    parms['type'] = 'video'
    parms['maxResults'] = '50'
    parms['relatedToVideoId'] = bases

    return parms

def get_search_results(parms, ctx):
    """Get search result for one query."""
    url = ENDPOINT + urllib.parse.urlencode(parms)
    print(f'Getting URL: {url}')
    search_results = urllib.request.urlopen(url,context=ctx)
    search_results_data = search_results.read().decode()
    return search_results_data

def get_video_list(col_name,table):
    """Get the list of all videos from search results file."""
    video_ids_list = []
    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()
    video_rows = cur.execute('SELECT ' + col_name +' FROM ' + table)

    for row in video_rows:
        video_ids_list += [video for video in row if video not in video_ids_list]
    
    return video_ids_list

def reduce_list(full_list, extracted):
    """reduce a full list based on what is already extracted.
    With this method, we are not accounting for temporal changes in related videos."""
    return [item for item in full_list if item not in extracted]

def write_to_file(search_results,bases):
    """Write a file containing search results, and append succeeding results."""
    filename = FILE_FOR_SEARCHES+bases+".txt"
    outfile = open(os.path.join(SAVEPATH,filename),'w',encoding = 'utf-8')
    outfile.write(search_results+"\n")
    outfile.close()

def main():
    """Initialize parameters, loop over a series of searches
    by getting search results, writing them, and moving to the next page."""

    ctx = get_url_ctx()
    query = set_query('Insert search query: ')

    searches = 0
    search_results = None
    pairs_of_results = []
    result_count = 0

    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()
    
    #Uncomment the next line if you want to reset the table
    #cur.execute('''DROP TABLE IF EXISTS related_searches''')
    cur.execute('''CREATE TABLE IF NOT EXISTS related_searches(base TEXT, video_id TEXT, rank INTEGER, ref_count INTEGER)''')

    video_list_to_pull = get_video_list('video_id','search_results')
    video_list_extracted = get_video_list('base','related_searches')
    video_list_to_pull = reduce_list(video_list_to_pull,video_list_extracted)
    
    for bases in video_list_to_pull[:LIMITER]:
        parms = set_url_parameters(query,bases)
        searches += 1
        print(f'Getting search #{searches}')
        search_results = get_search_results(parms,ctx)
        write_to_file(search_results,bases)
        search_results = json.loads(search_results)
        
        rank = 0
        for row in search_results['items']:
            rank += 1
            video_id = row['id']['videoId']
            cur.execute('SELECT ref_count FROM related_searches WHERE video_id = ? AND base = ? LIMIT 1',(video_id,bases))
            try:
                count = cur.fetchone()[0]
                count += 1
                cur.execute('''UPDATE related_searches SET ref_count = ? where video_id = ? AND base = ?''',(count,video_id,bases))
                print("Updating:", bases, video_id, rank, result_count, count)
            except:
                cur.execute('''INSERT INTO related_searches(base, video_id, rank, ref_count) VALUES (?,?,?,?)''',(bases,video_id,rank,1))
                print("inserting:", bases, video_id, rank, 1)
            result_count += 1
            pairs_of_results.append((video_id,bases))
        conn.commit()
        time.sleep(5)
    unique_pairs = len(set(pairs_of_results))
    print(f'From the {searches} searches, there were {result_count} videos wherein {unique_pairs} were unique')
    cur.close()

main()