"""Access retrieved channel data, and store them on SQLITE."""

import json
import sqlite3
import os.path

SAVEPATH = 'INSERT SAVEPATH WHERE THE TEXT FILES ARE'
FILE_FOR_SEARCHES = 'Channel_data.txt'

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

def get_channel_info(json_list):
    """Take one block of the channel data string and create a list of tuples."""
    channel_id = None
    title = None
    pub_date = None
    description = None
    view_count = None
    subscriber_count = None
    video_count = None
    all_channels = []
    
    for json_obj in json_list:
        channel_list = []
        for item in json_obj['items']:
            channel_id = item['id']
            title = item['snippet']['title']
            pub_date = item['snippet']['publishedAt']
            description = item['snippet']['description']
            view_count = item['statistics']['viewCount']
            subscriber_count = item['statistics']['subscriberCount']
            video_count = item['statistics']['videoCount']
        
            try:
                custom_url = item['snippet']['customUrl']
            except:
                custom_url = ''
        
            try:
                country = item['brandingSettings']['channel']['country']
            except:
                country = ''
        
            try:
                made_for_kids = item['status']['madeForKids']
            except:
                made_for_kids = ''
        
            try:
                branding_keywords = item['brandingSettings']['channel']['keywords']
            except:
                branding_keywords = ''
        
            try:
                topic_categories = item['topicDetails']['topicCategories']
            except:
                topic_categories = ''

            channel_list.append((channel_id, title, pub_date, description, view_count, subscriber_count, video_count, custom_url,country,made_for_kids,str(branding_keywords),str(topic_categories)))
        all_channels += channel_list
    return all_channels

def add_to_db(videos_list):
    """Add channel data to sqlite."""
    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()

    #If you want to reset the table
    #cur.execute('''DROP TABLE IF EXISTS channel_data''')
    cur.execute('''CREATE TABLE IF NOT EXISTS channel_data(
    channel_id TEXT, title TEXT, description TEXT,
    custom_url TEXT, country TEXT, pub_date TEXT, made_for_kids TEXT,
    topic_categories TEXT, branding_keywords TEXT, view_count INTEGER,
    subscriber_count INTEGER, video_count INTEGER)''')

    for row in videos_list:
        cur.execute('SELECT channel_id FROM channel_data WHERE channel_id = ? LIMIT 1',(row[0],))
        try:
            count = cur.fetchone()[0]
            print("Updating:", row[0])
        except:
            cur.execute('''INSERT INTO channel_data(channel_id, title, pub_date,
            description, view_count, subscriber_count, video_count, custom_url,
            country, made_for_kids, branding_keywords, topic_categories)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
            row)
            print("Inserting:", row[0])
    conn.commit()

def main():
    """Create a list of JSONs and then parse them to get channel info and add them to SQLITE."""
    json_list = read_file()
     
    print('Creating list of channels...')
    channel_list = get_channel_info(json_list)
     
    print('Adding to database...')
    add_to_db(channel_list)

main()