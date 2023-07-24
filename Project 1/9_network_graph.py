"""Create network graph to display connected videos"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import sqlite3

def videos_df():
    """Access the related_searches table to get relationships between videos,
    and store contents as a dataframe"""
    conn = sqlite3.connect('search_results.sqlite')
    cur = conn.cursor()
    sql_query = """
    SELECT base, video_id, rank, ref_count
    FROM related_searches"""

    videos= cur.execute(sql_query)
    headers = [head[0] for head in videos.description]

    df = pd.DataFrame(data = videos, columns = headers)
    df = df.replace('', 0)
    specify_type = {'rank': 'Int64',
                    'ref_count': 'Int64'}

    df = df.astype(specify_type)
    return df

def main():
    """Create the dataframe, and create a network graph"""
    video_df = videos_df()
    G=nx.from_pandas_edgelist(video_df[:120], 'base', 'video_id')
    nx.draw(G, with_labels=True)
    plt.show()

main()