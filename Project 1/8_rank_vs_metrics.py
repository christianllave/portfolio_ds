"""Create a simple graph using video metrics."""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter

def videos_df():
    """Join rank, video_id on basic video details for plotting,
    returned in the form of a dataframe"""
    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()
    sql_query = """
    SELECT rank, video_id, video_title, channel_title, view_count, like_count, comment_count
    FROM search_results
    LEFT JOIN video_data
    USING(video_id)"""

    videos= cur.execute(sql_query)
    headers = [head[0] for head in videos.description]

    df = pd.DataFrame(data = videos, columns = headers)
    df = df.replace('', 0)
    specify_type = {'view_count': 'Int64',
                    'like_count': 'Int64',
                    'comment_count': 'Int64'}

    df = df.astype(specify_type)
    return df

def process_df(video_df):
    """Add Engagement Rate Column. Reduce df to Engagement rate vs views."""
    engagements = video_df['like_count'] + video_df['comment_count']
    engagement_rate = engagements / video_df['view_count']
    new_df = video_df.assign(engagement_rate = engagement_rate)
    to_drop = ['video_id', 'video_title', 'channel_title', 'like_count', 'comment_count']
    new_df = new_df.drop(columns = to_drop, axis = 1)
    return new_df

def millions(x,pos):
    "The two args are the value and tick position"
    return '%1.1fM' % (x*1e-6)

def plot_rank_vs_metrics(reduced_df):
    """Plot rank vs views and engagement rate."""
    ax1 = reduced_df.plot(x = 'rank', y = 'view_count', label = 'View Count')
    ax2 = reduced_df.plot(x = 'rank', y = 'engagement_rate', label = 'Engagement Rate', color = 'Red', secondary_y = True, ax = ax1)

    format = FuncFormatter(millions)

    plt.title("Rank vs Video Count, Engagement Rate")
    ax1.set_xlabel('Rank')
    ax1.set_ylabel('View Count')
    ax2.set_ylabel('Engagement Rate')
    ax1.ticklabel_format(style = 'plain')
    ax1.yaxis.set_major_formatter(format)
    ax2.yaxis.set_major_formatter(PercentFormatter(1.0))


    plt.show()

def main():
    """Run program"""
    video_df = videos_df()
    reduced_df = process_df(video_df)
    plot_rank_vs_metrics(reduced_df)

main()