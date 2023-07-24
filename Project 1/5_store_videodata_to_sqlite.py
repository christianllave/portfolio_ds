"""Access retrieved video data, and store them on SQLITE."""

import json
import sqlite3
import os.path

SAVEPATH = 'INSERT SAVEPATH WHERE THE TEXT FILES ARE'
FILE_FOR_SEARCHES = 'Video_data.txt'

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
    """Take one block of the video data string and create a list of tuples."""
    video_id = None
    channel_id = None
    channel_title = None
    pub_date = None
    video_title = None
    video_description = None
    video_tags = None
    category_id = None
    view_count = 0
    like_count = 0
    comment_count = 0
    all_videos = []
    
    for json_obj in json_list:
        videos_list = []
        for item in json_obj['items']:
            video_id = item['id']
            channel_id = item['snippet']['channelId']
            channel_title = item['snippet']['channelTitle']
            pub_date = item['snippet']['publishedAt']
            video_title = item['snippet']['title']
            video_description = item['snippet']['description']
            try:
                video_tags = item['snippet']['tags']
            except:
                video_tags = ''
            category_id = item['snippet']['categoryId']
            view_count = item['statistics']['viewCount']
            try:
               like_count = item['statistics']['likeCount']
            except:
               like_count = 0
            try:
               comment_count = item['statistics']['commentCount']
            except:
               comment_count = 0
            videos_list.append((video_id,channel_id, channel_title, pub_date, video_title, video_description, str(video_tags), category_id, view_count, like_count, comment_count))
        all_videos += videos_list
    return all_videos

def add_to_db(videos_list):
    """Add video data to sqlite."""
    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()

    #Uncomment the next line if you want to reset the table
    #cur.execute('''DROP TABLE IF EXISTS video_data''')
    cur.execute('''CREATE TABLE IF NOT EXISTS video_data(
        video_id TEXT, channel_id TEXT, channel_title TEXT, pub_date TEXT,
        video_title TEXT, video_description TEXT, video_tags TEXT,
        category_id TEXT, view_count INTEGER, like_count INTEGER, comment_count INTEGER)''')

    for row in videos_list:
        cur.execute('SELECT video_id FROM video_data WHERE video_id = ? LIMIT 1',(row[0],))
        try:
            count = cur.fetchone()[0]
            print("Updating:", row[0])
        except:
            cur.execute('''INSERT INTO video_data(video_id,
            channel_id, channel_title, pub_date, video_title,
            video_description,video_tags,category_id,
            view_count,like_count,comment_count)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            row)
            print("Inserting:", row[0])
    conn.commit()

def main():
    """Create a list of JSONs and then parse them to get video info and add them to SQLITE."""
    json_list = read_file()
     
    print('Creating list of videos...')
    videos_list = get_video_info(json_list)
     
    print('Adding to database...')
    add_to_db(videos_list)

main()