import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
from mysql.connector import Error
import traceback

API_KEY = 'AIzaSyCjN9j5lj4-_epuduBzPt2In6xe5mYUmC4'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# Function to get channel info
def get_channel_info(channel_name):
    search_response = youtube.search().list(
        q=channel_name,
        type="channel",
        part="snippet",
        maxResults=1
    ).execute()

    if not search_response['items']:
        return None

    channel_id = search_response['items'][0]['snippet']['channelId']
    channel_response = youtube.channels().list(
        id=channel_id,
        part="snippet,statistics,contentDetails,status"
    ).execute()

    if not channel_response['items']:
        return None

    channel = channel_response['items'][0]
    channel_info = {
        'Title': channel['snippet']['title'],
        'Description': channel['snippet']['description'],
        'Channel ID': channel['id'],
        'View Count': channel['statistics']['viewCount'],
        'Subscriber Count': channel['statistics']['subscriberCount'],
        'Video Count': channel['statistics']['videoCount'],
        'Privacy Status': channel['status']['privacyStatus']
    }

    return channel_info

# Function to get all playlists for a channel
def get_all_playlists_for_channel(channel_id):
    all_playlists = []

    request = youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50
    )

    while request is not None and len(all_playlists) < 5:
        response = request.execute()

        for playlist in response.get('items', []):

            if len(all_playlists) < 5:
                all_playlists.append({
                    "Playlist_Id": playlist['id'],
                    "Playlist_Name": playlist['snippet']['title'],
                    "Published_At": playlist['snippet']['publishedAt'],
                    "Channel_Id": channel_id
                })
            else:
                break

        if len(all_playlists) < 5:
            request = youtube.playlists().list_next(request, response)
        else:
            request = None

    all_playlists.sort(key=lambda x: x['Published_At'], reverse=True)

    return all_playlists[:5]

# Function to get video info for a playlist
def get_video_info(playlist_id):
    video_data = {}
    playlist_videos_response = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=25
    ).execute()

    for item in playlist_videos_response.get('items', []):
        video_id = item['contentDetails']['videoId']
        video_detail_response = youtube.videos().list(
            id=video_id,
            part="snippet,statistics,contentDetails"
        ).execute()

        if video_detail_response['items']:
            video_detail = video_detail_response['items'][0]

            video_data[video_id] = {
                "Playlist_Id": playlist_id,
                "Video_Id": video_id,
                "Video_Name": video_detail['snippet']['title'],
                "Video_Description": video_detail['snippet']['description'],
                "PublishedAt": video_detail['snippet']['publishedAt'],
                "View_Count": video_detail['statistics'].get('viewCount', 0),
                "Like_Count": video_detail['statistics'].get('likeCount', 0),
                "Dislike_Count": video_detail['statistics'].get('dislikeCount', 0),
                "Favorite_Count": video_detail['statistics'].get('favoriteCount', 0),
                "Comment_Count": video_detail['statistics'].get('commentCount', 0),
                "Duration": video_detail['contentDetails']['duration'],
                "Thumbnail": video_detail['snippet']['thumbnails']['default']['url']
            }
    return video_data

# Function to get comments for a video
def get_comments(video_id):
    comments_response = youtube.commentThreads().list(part="snippet", videoId=video_id,
                                                      maxResults=100).execute()
    comments = []

    for item in comments_response['items']:
        comment = item['snippet']['topLevelComment']
        comment_snippet = comment['snippet']

        comments.append({
            "Comment_Id": comment['id'],
            "Video_Id": video_id,
            "Comment_Text": comment_snippet['textDisplay'],
            "Comment_Author": comment_snippet['authorDisplayName'],
            "Comment_PublishedAt": comment_snippet['publishedAt']
        })

    return comments

# Function to execute SQL queries
def execute_sql_query(query):
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456789',
            database='Youtube'
        )
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as e:
        st.error(f"Error while executing SQL query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Function to insert channel info, playlists, videos, and comments into MySQL
def insert_channel_info(channel_info, playlist_info_list, video_data, comments):
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456789',
            database='Youtube'
        )
        cursor = connection.cursor()

        # Insert channel info
        query_channel = """
        INSERT INTO channel
        (channel_id, channel_Name, channel_views, channel_status, channel_description)
        VALUES (%s, %s, %s, %s, %s)
        """
        data_tuple = (
            channel_info['Channel ID'],
            channel_info['Title'],
            channel_info['View Count'],
            channel_info['Privacy Status'],
            channel_info['Description']
        )
        cursor.execute(query_channel, data_tuple)

        # Insert playlist info
        query_playlist = """
        INSERT INTO playlist
        (playlist_id, channel_id, playlist_name) VALUES (%s, %s, %s)
        """
        for playlist_info in playlist_info_list:
            data_playlist = (
                playlist_info['Playlist_Id'],
                channel_info['Channel ID'],
                playlist_info['Playlist_Name']
            )
            cursor.execute(query_playlist, data_playlist)

        # Insert video data
        query_video = """
        INSERT INTO Video
        (Playlist_Id, Video_Id, Video_Name, Video_Description, published_date, View_Count, 
        Like_Count, Favorite_Count, Comment_Count, Duration, Thumbnail) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for video_id, video_info in video_data.items():
            video_details = (
                video_info['Playlist_Id'],
                video_info['Video_Id'],
                video_info['Video_Name'],
                video_info['Video_Description'],
                video_info['PublishedAt'],
                int(video_info['View_Count']),
                int(video_info['Like_Count']),
                int(video_info['Favorite_Count']),
                int(video_info['Comment_Count']),
                video_info['Duration'],
                video_info['Thumbnail']
            )
            cursor.execute(query_video, video_details)
        query_comments = """
        INSERT INTO Comment
        (Comment_Id, Video_Id, Comment_Text, Comment_Author, comment_published_date) 
        VALUES (%s, %s, %s, %s, %s)
        """
        for comment in comments:
            data_comment = (
                comment['Comment_Id'],
                comment['Video_Id'],
                comment['Comment_Text'],
                comment['Comment_Author'],
                comment['Comment_PublishedAt']
            )
            cursor.execute(query_comments, data_comment)

        connection.commit()
        st.success("Channel info inserted successfully")

    except mysql.connector.Error as e:
        st.error("Error while connecting to MySQL", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Streamlit app
st.title('YouTube Channel Data Fetcher')

channel_name = st.text_input('Enter YouTube Channel Name:', 'suntv')

if st.button('Fetch Channel Data'):
    with st.spinner('Fetching channel data...'):
        channel_data = get_channel_info(channel_name)
        Play_list = get_all_playlists_for_channel(channel_data['Channel ID'])
        
        videos_data = {}
        for playlist in Play_list:
            # Fetch video info for each playlist
            playlist_videos = get_video_info(playlist['Playlist_Id'])
            videos_data.update(playlist_videos)

        comments_data = []
        for video_id in videos_data:
            # Fetch comments for each video
            video_comments = get_comments(video_id)
            comments_data.extend(video_comments)

        insert_channel_info(channel_data, Play_list, videos_data, comments_data)

        channel_df = pd.DataFrame([channel_data])
        video_df = pd.DataFrame.from_dict(videos_data, orient='index')
        playlist_df = pd.DataFrame(Play_list)
        comments_df = pd.DataFrame(comments_data)

        st.success('Data fetched successfully!')

    # Display DataFrames
    st.subheader('Channel Data')
    st.dataframe(channel_df)

    st.subheader('Video Data')
    st.dataframe(video_df)

    st.subheader('Playlist Data')
    st.dataframe(playlist_df)

    st.subheader('Comments Data')
    st.dataframe(comments_df)

# SQL Query section
st.header("SQL Queries")
selected_query = st.selectbox("Select SQL Query", [
    "Query 1: Names of all videos and their corresponding channels",
    "Query 2: Channels with the most videos and their counts",
    "Query 3: Top 10 most viewed videos and their channels",
    "Query 4: Number of comments per video and video names",
    "Query 5: Videos with the highest number of likes and channel names",
    "Query 6: Total likes and dislikes per video and video names",
    "Query 7: Total views per channel and channel names",
    "Query 8: Channels that published videos in 2022",
    "Query 9: Average duration of videos per channel",
    "Query 10: Videos with the highest number of comments and channel names"
])

if st.button('Execute SQL Query'):
    if selected_query == "Query 1: Names of all videos and their corresponding channels":
        query = """
            SELECT v.video_name, c.channel_name
            FROM video v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id;
        """
    elif selected_query == "Query 2: Channels with the most videos and their counts":
        query = """
            SELECT c.channel_name, COUNT(v.video_id) AS video_count
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_name
            ORDER BY video_count DESC
            LIMIT 1;
        """
    elif selected_query == "Query 3: Top 10 most viewed videos and their channels":
        query = """
            SELECT v.video_name, c.channel_name, v.view_count
            FROM video v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            ORDER BY v.view_count DESC
            LIMIT 10;
        """
    elif selected_query == "Query 4: Number of comments per video and video names":
        query = """
            SELECT v.video_name, COUNT(cm.comment_id) AS comment_count
            FROM video v
            LEFT JOIN comment cm ON v.video_id = cm.video_id
            GROUP BY v.video_name;
        """
    elif selected_query == "Query 5: Videos with the highest number of likes and channel names":
        query = """
            SELECT v.video_name, c.channel_name, v.like_count
            FROM video v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            WHERE v.like_count = (SELECT MAX(like_count) FROM video);
        """
    elif selected_query == "Query 6: Total likes and dislikes per video and video names":
        query = """
            SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes
            FROM video v
            GROUP BY v.video_name;
        """
    elif selected_query == "Query 7: Total views per channel and channel names":
        query = """
            SELECT c.channel_name, SUM(v.view_count) AS total_views
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_name;
        """
    elif selected_query == "Query 8: Channels that published videos in 2022":
        query = """
            SELECT DISTINCT c.channel_name
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            WHERE YEAR(v.published_date) = 2022;
        """
    elif selected_query == "Query 9: Average duration of videos per channel":
        query = """
            SELECT c.channel_name, AVG(v.duration) AS avg_duration
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_name;
        """
    elif selected_query == "Query 10: Videos with the highest number of comments and channel names":
        query = """
            SELECT v.video_name, c.channel_name, COUNT(cm.comment_id) AS comment_count
            FROM video v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            LEFT JOIN comment cm ON v.video_id = cm.video_id
            GROUP BY v.video_name, c.channel_name
            HAVING comment_count = (SELECT MAX(comment_count) FROM comment);
        """


    result = execute_sql_query(query)

    if result:
        df = pd.DataFrame(result)
        st.dataframe(df)

# Add any additional Streamlit components you need.
