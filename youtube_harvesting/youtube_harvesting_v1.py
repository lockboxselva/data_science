import streamlit as st
import mysql.connector
import pandas as pd
import logging
import json
from logging.handlers import RotatingFileHandler
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo.mongo_client import MongoClient
from datetime import datetime
from mysql.connector import errorcode

# Configure logging
log_file = "youtube_harvesting.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=2)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

# Set your API key
api_key =  'AIzaSyCDKVBE1feDfh-scQv2wibBFN796Uzls5E'

# Set the YouTube API service
youtube_api = build('youtube', 'v3', developerKey=api_key)


def get_channel_info(channel_id):
    try:
        channel_request = youtube_api.channels().list(part='snippet,contentDetails,statistics,status', id=channel_id)
        channel_response = channel_request.execute()
        channel_info = channel_response['items'][0]
        logger.info(f"channel_info => {channel_info}")
        snippet = channel_info.get('snippet', {})
        statistics = channel_info.get('statistics', {})
        status = channel_info.get('status', {})
        kind = channel_info.get('kind', {})
        contentDetails = channel_info.get('contentDetails',{})

        channel_data = dict(
                        _id=channel_info.get('id'),
                        Channel_Name=snippet.get('title'),
                        Subscription_Count=statistics.get('subscriberCount'),
                        Channel_Views=statistics.get('viewCount'),
                        Channel_Type=kind.split('#')[-1],
                        Channel_Status=status.get('privacyStatus'),
                        Channel_Total_videos=statistics.get('videoCount'),
                        Channel_Description=snippet.get('description'),
                        Playlist_Id=contentDetails.get('relatedPlaylists').get('uploads')
                        )
        return channel_data
    except Exception as e:
        print(f"Error getting channel information: {e}")

def get_playlists(channel_id):
    next_page_token =  None
    playlists = []
    try:
        while True:
            playlist_response = youtube_api.playlists().list(part='snippet,contentDetails', channelId=channel_id, maxResults=50,pageToken = next_page_token).execute()
            playlists.extend(playlist_response['items'])
            logger.info(f"playlists => {playlists}")
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break
        return playlists
    except Exception as e:
        print(f"Error getting playlists: {e}")

def get_videos(playlist_id):
    logger.info(f"get_videos =========>")
    next_page_token =  None
    video_list = []
    try:
        while True:
            playlistitem_response = youtube_api.playlistItems().list(part='snippet,contentDetails', playlistId=playlist_id).execute()
            logger.info(f"video_response =========> {playlistitem_response}")
            for playlistItem in playlistitem_response['items']:
                video_id = playlistItem['contentDetails']['videoId']
                video_response = youtube_api.videos().list(part = 'snippet, contentDetails, statistics',id = video_id).execute()
                for item in video_response['items']:
                    print("\n\nvideo_detail : ", item)
                    data = dict(#Channel_Name = item['snippet']['channelTitle'],
                            #Channel_Id = item['snippet']['channelId'],
                            Video_Id = item['id'],
                            Video_Name = item['snippet']['title'],
                            Tags = item['snippet'].get('tags'),
                            Thumbnail = item['snippet']['thumbnails']['default']['url'],
                            Video_Description = item['snippet'].get('description'),
                            PublishedAt=item['snippet']['publishedAt'],
                            Duration = item['contentDetails']['duration'],
                            View_Count = item['statistics'].get('viewCount'),
                            Like_Count = item['statistics'].get('likeCount') , #check here
                            Dislike_Count = item['statistics'].get('dislikeCount') , #check here
                            Comment_Count = item['statistics'].get('commentCount'),
                            Favorite_Count = item['statistics']['favoriteCount'],
                            #video_definition_type = item['contentDetails']['definition'],
                            Caption_Status = item['contentDetails']['caption']
                            )
                video_list.append(data)
            #logger.info(f"videos => {videos}")
            next_page_token = video_response.get('nextPageToken')
            if not next_page_token:
                break
            print(f"video details fetched scuccessfully!")
        return video_list
    except Exception as e:
        print(f"Error getting videos: {e}")

def get_comments(video_id):
    next_page_token =  None
    comments_list = []
    try:        
        while True:
            comment_response = youtube_api.commentThreads().list(part='snippet', videoId=video_id, maxResults=50,pageToken = next_page_token).execute()
            for item in comment_response['items']:
                logger.info(f"Comment => {item}")
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt']                    
                            )
                comments_list.append(data)
            next_page_token = comment_response.get('nextPageToken')
            if not next_page_token:
                break
    except HttpError as e:
                print(e)
                if e.resp.status == 403:
                    error_reason = e.error_details[0]['reason']
                    if error_reason == 'commentsDisabled':
                        logger.error(f"Comments disabled for the video ID: {video_id}")
                        #continue  # Skip to the next video
                    else:
                        Exception(e)
                else:
                    Exception(e)
    except Exception as e:
        print(f"Error getting comments: {e}")
    return comments_list


def build_channel_details(channel_id, db):
    try:
        channel_info = get_channel_info(channel_id)
        logger.info(f"channel_info ====> {channel_info}")
        playlists = get_playlists(channel_id)
        logger.info(f"playlists ====> {playlists}")
        playlist_list = []
        for playlist in playlists:
            playlist_id = playlist['id']
            playlist_data = dict(
                            playlist_id = playlist_id,
                            playlist_title = playlist['snippet']['title'],
                            published_at = playlist['snippet']['publishedAt'],
                            playlist_video_count = playlist['contentDetails']['itemCount'] if 'contentDetails' in playlist else None)
            #print("\tPlaylist ID:",playlist['id'])
            videos = get_videos(playlist_id)
            logger.info(f"videos ========> {videos}")
            video_list = []
            if videos:
                for item in videos:
                    video_data = dict(
                                    Video_Id = item['id'],
                                    Playlist_Id = playlist_id,
                                    Video_Name = item['snippet']['title'],
                                    Tags = item['snippet'].get('tags'),
                                    Thumbnail = item['snippet']['thumbnails']['default']['url'],
                                    Video_Description = item['snippet'].get('description'),
                                    PublishedAt=item['snippet']['publishedAt'],
                                    Duration = item['contentDetails']['duration'] if 'contentDetails' in item else None,
                                    View_Count = item['statistics'].get('viewCount') if 'statistics' in item else None,
                                    Like_Count = item['statistics'].get('likeCount') if 'statistics' in item else None , 
                                    Dislike_Count = item['statistics'].get('dislikeCount') if 'statistics' in item else None, 
                                    Comment_Count = item['statistics'].get('commentCount') if 'statistics' in item else None,
                                    Favorite_Count = item['statistics']['favoriteCount'] if 'statistics' in item else None,
                                    Caption_Status = item['contentDetails']['caption'] if 'contentDetails' in item else None
                                    )
                    print("**********************1")
                    video_id = item['snippet']['resourceId']['videoId']
                    #print(item['id'], video_id)
                    comments = get_comments(video_id)   
                    video_data['comments'] = comments 
                    video_list.append(video_data)
                    #print("\t\tvideo:",video)
            playlist_data['videos']=video_list
            playlist_list.append(playlist_data)
        channel_info['playlist']=playlist_list
                #logger.info(f"Fetched channel_details : {channel_details}")
            
        collection_channel = db["channelDetails"]
        collection_channel.insert_one(channel_info)
        logger.info("\nChannel Information saved to MongoDB!")
    except Exception as e:
        logger.info(e)
        raise Exception(f"Error building channel details: {e}")
    return "Channel Information saved to MongoDB!"


def load_channel_data_to_SQL(channel_info):
    sql_db = connect_to_sql()
    cursor = sql_db.cursor(buffered=True)
  
    insert_query='''INSERT INTO channel(
                                    `channel_id`,
                                    `channel_name`,
                                    `channel_description`,
                                    `channel_status`,
                                    `channel_type`,
                                    `channel_views`,
                                    `channel_video_count`)
                    VALUES( %s,%s,%s,%s,%s,%s,%s)'''
    values = (channel_info['_id'],
            channel_info['Channel_Name'],
            channel_info['Channel_Description'],
            channel_info['Channel_Status'],
            channel_info['Channel_Type'],
            channel_info['Channel_Views'],
            channel_info['Channel_Total_videos'])
    try:
        cursor.execute(insert_query,values)
        sql_db.commit()
        logger.info("\nChannel information saved successfully!")
    except mysql.connector.Error as err:
        logger.info(err)
        if err.errno == errorcode.ER_DUP_ENTRY:
            logger.info(f"Duplicate entry for channel ID: {channel_info['_id']}. Skipping...")
        else:
            raise
    except Exception as e:
        logger.info(f"{channel_info['_id']}: {e}")
        raise Exception(e)
    return "Channel information saved successfully!"

def load_playlist_data_to_SQL(channel_info):
    sql_db=connect_to_sql()
    cursor = sql_db.cursor(buffered=True)
    playlist_data = []
    for playList in channel_info['playlist']:
        logger.info(f"PLAYLIST : {playList}")
        playlist_id = playList.get('playlist_id')
        channel_id = channel_info.get('_id')
        playlist_title = playList.get('playlist_title')    
        #logger.info(f"{playlist_id}, {channel_id}, {playlist_title}")

        if not playlist_id or not channel_id or not playlist_title:
            logger.info(f"Skipping {playlist_id} due to missing values.")
            continue

        playlist_data.append((playlist_id, channel_id, playlist_title))
    insert_query = '''INSERT INTO `playlist`(
                            `playlist_id`,
                            `channel_id`,
                            `playlist_name`)
                    VALUES(%s, %s, %s)'''
        
    try:
        cursor.executemany(insert_query, playlist_data)
        sql_db.commit()
        logger.info("\nPlaylist information saved successfully!")
    except mysql.connector.Error as err:
        logger.info(err)
        if err.errno == errorcode.ER_DUP_ENTRY:
            logger.info(f"Duplicate entry for playlist ID. Skipping...")
        else:
            raise Exception(err)
    except Exception as e:
        logger.info(f"Error inserting playlists: {e}")
        raise Exception(e)

    return "Playlist information saved successfully!"

def load_video_data_to_SQL(channel_info):
    sql_db = connect_to_sql()
    cursor = sql_db.cursor(buffered=True)
    video_data_list = []

    playlists = channel_info['playlist']
    for playlist in playlists:
        for video in playlist['videos']:
            video_id = video.get('Video_Id')
            playlist_id = video.get('Playlist_Id')
            video_name = video.get('Video_Name')
            video_description = video.get('Video_Description')
            input_datetime = datetime.strptime(video.get('PublishedAt'), '%Y-%m-%dT%H:%M:%SZ')
            published_date = input_datetime.strftime('%Y-%m-%d %H:%M:%S')
            view_count = video.get('View_Count', 0)
            like_count = video.get('Like_Count', 0)
            dislike_count = video.get('Dislike_Count', 0)
            favorite_count = video.get('Favorite_Count', 0)
            comment_count = video.get('Comment_Count', 0)

            # Extract minutes and seconds from the input string
            minutes = 0
            seconds = 0
            duration_str = video.get('Duration')
            if not duration_str:
                duration = 0
            else:
                if 'M' in duration_str:
                    minutes_index = duration_str.index('M')
                    minutes = int(duration_str[2:minutes_index])

                if 'S' in duration_str:
                    seconds_index = duration_str.index('S')
                    seconds_str = duration_str[minutes_index + 1:seconds_index]
                    seconds = int(seconds_str) if seconds_str else 0

                # Calculate total duration in seconds
                total_seconds = minutes * 60 + seconds
                # Create a timedelta object
                duration = total_seconds

            thumbnail = video['Thumbnail']
            caption_status = video.get('Caption_Status', '')

            video_data = (video_id, playlist_id, video_name, video_description, published_date,
                          view_count, like_count, dislike_count, favorite_count,
                          comment_count, duration, thumbnail, caption_status)

            video_data_list.append(video_data)

    insert_query = '''INSERT INTO `videos`(
                        video_id, playlist_id, video_name, video_description, published_date,
                        view_count, like_count, dislike_count, favorite_count,
                        comment_count, duration, thumbnail, caption_status)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    try:
        cursor.executemany(insert_query, video_data_list)
        sql_db.commit()
        logger.info("\nVideo information saved successfully!")
    except mysql.connector.Error as err:
        logger.info(err)
        if err.errno == errorcode.ER_DUP_ENTRY:
            logger.info(f"Duplicate entry for Video ID. Skipping...")
        else:
            raise Exception(err)
    except Exception as e:
        logger.info(f"Error inserting videos: {e}")
        raise Exception(e)

    return "Video information saved successfully!"

def load_comments_data_to_SQL(channel_info):
    sql_db = connect_to_sql()
    cursor = sql_db.cursor(buffered=True)

    comment_data_list = []

    for playlist in channel_info['playlist']:
        for video in playlist['videos']:
            if video['comments']:
                for comment in video['comments']:
                    video_id = video.get('Video_Id')
                    comment_id = comment.get('Comment_Id')
                    comment_text = comment.get('Comment_Text')
                    comment_author = comment.get('Comment_Author')
                    input_datetime = datetime.strptime(comment.get('Comment_PublishedAt'), '%Y-%m-%dT%H:%M:%SZ')
                    comment_published_date = input_datetime.strftime('%Y-%m-%d %H:%M:%S')

                    comment_data = (comment_id, video_id, comment_text, comment_author, comment_published_date)
                    comment_data_list.append(comment_data)

    insert_query = '''INSERT INTO `comment`(
                        comment_id, video_id, comment_text, comment_author, comment_published_date)
                    VALUES (%s, %s, %s, %s, %s)'''

    try:
        cursor.executemany(insert_query, comment_data_list)
        sql_db.commit()
        logger.info("\nComments information saved successfully!")
    except mysql.connector.Error as err:
        logger.info(err)
        if err.errno == errorcode.ER_DUP_ENTRY:
            logger.info(f"Duplicate entry for Comment ID. Skipping...")
        else:
            raise Exception(err)
    except Exception as e:
        logger.info(f"Error inserting comments: {e}")
        raise Exception(e)

    return "Comments information saved successfully!"


def show_channels_table():
    channelList = []
    channelDetails = db["channelDetails"]

    for channel in channelDetails.find({}):
        channel_info={}
        channel_info['Id']=channel['_id']
        channel_info['Name']=channel['Channel_Name']
        channel_info['Description']=channel['Channel_Description']
        channel_info['Total_videos']=channel['Channel_Total_videos']
        channel_info['Total_Views']=channel['Channel_Views']
        channel_info['Channel_Type']=channel['Channel_Type']
        channel_info['Subscription_Count']=channel['Subscription_Count']
        channel_info['Status']=channel['Channel_Status']
        channelList.append(channel_info)
    return channelList

def show_playlist_table():
    playLists = []
    channelDetails = db["channelDetails"]

    for channel in channelDetails.find({}):
        for play in channel['playlist']:
            playlist ={}
            playlist['Channel_Id'] = channel['_id']
            playlist['Playlist_Id'] = play['playlist_id']
            playlist['Playlist_Title'] = play['playlist_title']
            playlist['Playlist_Video_Count'] = play['playlist_video_count']
            playlist['Published_At'] = play['published_at']
            playLists.append(playlist)
    return playLists

def show_video_table():    
    videoList = []
    channelDetails = db["channelDetails"]

    for channel in channelDetails.find({}):
        for playlist in channel['playlist']:
            for video in playlist['videos']:    
                video_info ={}
                video_info['Video_Id'] = video['Video_Id']
                video_info['Video_Name'] = video['Video_Name']
                video_info['Video_Description'] = video['Video_Description']
                video_info['Channel_Id'] = channel['_id']
                video_info['Playlist_Id'] = video['Playlist_Id']
                video_info['Tags'] = video['Tags']
                video_info['Thumbnail'] = video['Thumbnail']
                video_info['PublishedAt'] = video['PublishedAt']
                video_info['Duration'] = video['Duration']
                video_info['View_Count'] = video['View_Count']
                video_info['Like_Count'] = video['Like_Count']
                video_info['Dislike_Count'] = video['Dislike_Count']
                video_info['Comment_Count'] = video['Comment_Count']
                video_info['Favorite_Count'] = video['Favorite_Count']
                video_info['Caption_Status'] = video['Caption_Status']
                videoList.append(video_info)
    return videoList

def show_comments_table():  
    commentList = []
    channelDetails = db["channelDetails"]
    for channel in channelDetails.find({}):
        for playlist in channel['playlist']:
            for video in playlist['videos']:  
                if not video['comments']:
                    continue  # Skip videos with no comments
                else:
                    for comment in video['comments']:
                        comment_info = {}
                        comment_info['Comment_Id']= comment['Comment_Id']
                        comment_info['Video_Id']= comment['Video_Id']
                        comment_info['Comment_Text']= comment['Comment_Text']
                        comment_info['Comment_Author']= comment['Comment_Author']
                        comment_info['Comment_PublishedAt']= comment['Comment_PublishedAt']
                        commentList.append(comment_info)
    return commentList

def show_table_data(table_data):
    return st.dataframe(table_data)

def connect_to_sql():
     # SQL Connection
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Summer1$",
        database='youtube_harvesting'
    )
    return db

def main():
    st.title(":blue[Youtube Data Harvesting]")
    st.header("Extract data from Youtube")
    db = client["youtube"]
    channel_id = st.text_input("Enter the channel ID")
    if st.button("Extract"):
        
        if not channel_id:
            st.warning("Please enter a valid Channel ID.")
        else:
            ch_ids = []
            collection_channel = db["channelDetails"]
            for ch_data in collection_channel.find({"_id":channel_id},{"_id":1}):
                ch_ids.append(ch_data['_id'])

            if channel_id in ch_ids:
                st.warning("Channel details already exist.")
            else:
                with st.spinner("Extracting data..."):
                    try:
                        logger.info("\n*******Extraction - START*******")
                        result = build_channel_details(channel_id, db)
                        logger.info("\n*******Extraction - END*********")
                        st.success(result)
                    except Exception as e:
                        print(e)
                        st.warning(f"Exception Occurred: {e}")
                    
    if st.button("Transform"):
        try:
            with st.spinner("Processing data..."):
                logger.info("\n*******Transform - START*******")
                db = client["youtube"]
                channelDetails = db["channelDetails"]
                channel_info = channelDetails.find_one({"_id":channel_id})
                load_channel_data_to_SQL(channel_info)
                logger.info("\n*******CHANNEL - END*******")
                load_playlist_data_to_SQL(channel_info)
                logger.info("\n*******PLAYLIST - END*******")
                load_video_data_to_SQL(channel_info)
                logger.info("\n*******VIDEO - END*******")
                load_comments_data_to_SQL(channel_info)
                logger.info("\n*******Transform - END*********")
            st.success("Transformation completed successfully!")
        except Exception as e:
            logger.error(e)
            st.warning(f"Exception Occurred: {e}")
            

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("View Transformed Data")
    show_table = st.radio("Select Table", ("Channel", "Playlist", "Videos", "Comments"))

    if show_table == "Channel":
        show_table_data(show_channels_table())
    elif show_table == 'Playlist':
        show_table_data(show_playlist_table())
    elif show_table == 'Videos':
        show_table_data(show_video_table())
    elif show_table == 'Comments':
        show_table_data(show_comments_table())
    st.markdown("<hr>", unsafe_allow_html=True)

    sql_db = connect_to_sql()
    cursor = sql_db.cursor(buffered=True)
    cursor.execute("SELECT * FROM youtube_harvesting.questions ORDER BY id")
    result = cursor.fetchall()
    question_dict = [{"name": record[1], "id": record[0]} for record in result]
    query_dict = [{"id": record[0], "query": record[2]} for record in result]

    # Use the keys of question_dict to create a list for the selectbox
    question_names = [item["name"] for item in question_dict]
    st.header("View Analyzed Data")

    # Create a Streamlit selectbox
    selected_question = st.selectbox("Choose a question", question_names)

    # Get the corresponding ID and query based on the selected question
    selected_question_id = next(item["id"] for item in question_dict if item["name"] == selected_question)
    selected_query = next(item["query"] for item in query_dict if item["id"] == selected_question_id)

    cursor.execute(selected_query)
    result = cursor.fetchall()
    # Get column names from the cursor description
    column_names = [desc[0] for desc in cursor.description]
    data_frame = pd.DataFrame(result, columns=column_names)
    st.write(data_frame)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")
    db = client["youtube"]
    main()
