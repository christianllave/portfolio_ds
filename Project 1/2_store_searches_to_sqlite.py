"""Access retrieved search data, and store them on SQLITE."""

import json
import sqlite3
import os.path

SAVEPATH = 'INSERT SAVEPATH WHERE THE TEXT FILES ARE'
FILE_FOR_SEARCHES = 'Search_data.txt'

def read_file():
     """Get each JSON block and add them to a list of JSONs for parsing."""
     infile = open(os.path.join(SAVEPATH,FILE_FOR_SEARCHES),'r',encoding = 'utf-8')
     data = infile.read()
     braces = 0
     json_content = ''
     json_list = []
     results = None
     for json_obj in data:
          braces += json_obj.count('{')
          braces -= json_obj.count('}')
          json_content += json_obj

          if braces == 0 and json_obj != '\n':
               results = json.loads(json_content)
               json_list.append(results)
               json_content = ''

     infile.close()
     return json_list

def get_video_info(json_list):
     """Take one block of the search result string and convert to dictionary.
     We only take the video_id and channel_id because the response of a search request
     is limited to high-level information. This means we'll need to make a separate request
     to get data on a specific video."""
     video_id = None
     channel_id = None
     all_videos = []
     rank = 0
     for json_obj in json_list:
          videos_list = []
          for item in json_obj['items']:
               rank +=1
               video_id = item['id']['videoId']
               channel_id = item['snippet']['channelId']
               videos_list.append((rank,video_id,channel_id))
          all_videos += videos_list
     return all_videos

def add_to_db(videos_list):
     """Add video data to sqlite."""
     conn = sqlite3.connect('search_results.sqlite')
     cur = conn.cursor()
     
     #Uncomment the next line to reset the table
     #cur.execute('''DROP TABLE IF EXISTS search_results''')
     cur.execute('''CREATE TABLE IF NOT EXISTS search_results(rank INTEGER, video_id TEXT, channel_id TEXT)''')

     for rank,video_id,channel_id in videos_list:
          cur.execute('SELECT video_id FROM search_results WHERE video_id = ? LIMIT 1',(video_id,))
          try:
               count = cur.fetchone()[0]
               pass
          except:
               cur.execute('''INSERT INTO search_results(rank,video_id, channel_id) VALUES (?,?,?)''',(rank,video_id,channel_id))
     conn.commit()
     cur.close()

def main():
     """Create a list of JSONs and then parse them to get video info (video_id, channel_id)
     and add them to SQLITE."""
     json_list = read_file()
     print('Creating list of videos...')
     videos_list = get_video_info(json_list)
     print('Adding to database...')
     add_to_db(videos_list)
     num_videos = len(videos_list)
     unique_videos = len(set(videos_list))
     print(f'There were {num_videos} videos extracted, with {unique_videos} unique videos.')

main()