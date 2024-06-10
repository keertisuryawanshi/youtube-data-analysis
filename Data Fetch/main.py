import pandas as pd
from dateutil import parser
import isodate

from googleapiclient.discovery import build
from google.cloud import storage
# import os

# Set the environment variable for Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-management-1-ws2023-num-2-d2b0e58f01e9.json"

api_key = 'AIzaSyB07HWDkZ5hqeBBNf4G1Wn9g-SVh9dho1Y'

channel_ids = [
               'UCtYLUTtgS3k1Fg4y5tAhLbw', # Statquest
               'UCCezIgC97PvUuR4_gbFUs5g', # Corey Schafer
               'UCfzlCWGWYyIQ0aLC5w48gBQ', # Sentdex
               'UCNU_lfiiWBdtULKOw6X0Dig', # Krish Naik
               'UCzL_0nIe8B4-7ShhVPfJkgw', # DatascienceDoJo
               'UCLLw7jmFsvfIVaUFsLs8mlQ', # Luke Barousse
               'UCiT9RITQ9PW6BhXK0y2jaeg', # Ken Jee
               'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex the analyst
               'UC2UXDak6o7rBm23k3Vv5dww', # Tina Huang
               'UCnVzApLJE2ljPZSeQylSEyg', # Data School
               'UCh9nVJoWXmFb7sLApWGcLPQ'  # codebasics
]

youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(channelName = response['items'][i]['snippet']['title'],
                    subscribers = response['items'][i]['statistics']['subscriberCount'],
                    views = response['items'][i]['statistics']['viewCount'],
                    totalVideos = response['items'][i]['statistics']['videoCount'],
                    playlistId = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)

    return pd.DataFrame(all_data)

def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()

    video_ids = []
    for i in range(len(response['items'])):
        if i == 5:
            break
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    n = 0
    while more_pages:
        n += 1
        if n == 5:
            break

        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                print('Got video ID for video ' + response['items'][i]['contentDetails']['videoId'])
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids

def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        if i == 5:
            break
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            print('Got video info for video ' + video['id'])

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)

def get_comments_in_videos(youtube, video_ids):
    all_comments = []

    i = 0
    for video_id in video_ids:
        try:
            i += 1
            if i == 5:
                break

            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()

            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            print('Got comments for video ' + video_id)

            all_comments.append(comments_in_video_info)

        except:
            print('Could not get comments for video ' + video_id)

    return pd.DataFrame(all_comments)

def main(event, context):
    channel_data = get_channel_stats(youtube, channel_ids)

    numeric_cols = ['subscribers', 'views', 'totalVideos']
    channel_data[numeric_cols] = channel_data[numeric_cols].apply(pd.to_numeric, errors='coerce')

    video_df = pd.DataFrame()
    comments_df = pd.DataFrame()

    for c in channel_data['channelName'].unique():
        print("Getting video information from channel: " + c)
        playlist_id = channel_data.loc[channel_data['channelName']== c, 'playlistId'].iloc[0]
        video_ids = get_video_ids(youtube, playlist_id)

        video_data = get_video_details(youtube, video_ids)
        comments_data = get_comments_in_videos(youtube, video_ids)

        print("Number of videos: " + str(len(video_data)))
        print("Number of comments: " + str(len(comments_data)))

        video_df = pd.concat([video_df, video_data], ignore_index=True)
        comments_df = pd.concat([comments_df, comments_data], ignore_index=True)

    cols_to_convert = ['viewCount', 'likeCount', 'favouriteCount', 'commentCount']
    video_df[cols_to_convert] = video_df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

    video_df['publishedAt'] =  video_df['publishedAt'].apply(lambda x: parser.parse(x))
    video_df['publishDayName'] = video_df['publishedAt'].apply(lambda x: x.strftime("%A"))

    video_df['durationSecs'] = video_df['duration'].apply(lambda x: isodate.parse_duration(x))
    video_df['durationSecs'] = video_df['durationSecs'].astype('timedelta64[s]')

    video_df['tagsCount'] = video_df['tags'].apply(lambda x: 0 if x is None else len(x))

    video_df['likeRatio'] = video_df['likeCount']/ video_df['viewCount'] * 1000
    video_df['commentRatio'] = video_df['commentCount']/ video_df['viewCount'] * 1000

    video_df['titleLength'] = video_df['title'].apply(lambda x: len(x))

    print("number rows: ", video_df.shape[0])

    csv_filename = 'data.csv'
    video_df.to_csv(csv_filename, index=False)

    print(f"Data fetched successfully and written to '{csv_filename}'")

    bucket_name = 'de2_project_youtube_analysis'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f'{csv_filename}'

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_filename)

    print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")

    return "Process completed successfully."

if __name__ == "__main__":
    main(None)
