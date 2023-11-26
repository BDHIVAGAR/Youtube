import pandas as pd
import streamlit as st
import pymysql
import pymongo
from pymongo import MongoClient
import googleapiclient.discovery
from googleapiclient.discovery import build
import plotly.express as px

# BUILDING CONNECTION WITH YOUTUBE API
api_key= "AIzaSyCIQCuw6hfeoezEJDFA-q0i17Wle8yl_gA"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# MongoDB-existing database
client = pymongo.MongoClient('mongodb://localhost:27017')  
mydb = client["project"]
information = mydb.youtube

# Pushing data from mongodb into python
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['project']
information1 = db.youtube

# Bridging a connection with Mysql Database
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='dhiva58')
cur = myconnection.cursor()
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='dhiva58',database = "youtube")
cur = myconnection.cursor()

st.title("YouTube Data Harvesting and Warehousing")


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    channel_details = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    c_details = dict(channel_Id = response['items'][0]['id'],
                     channel_name = response['items'][0]['snippet']['title'],
                     channel_description = response['items'][0]['snippet']['localized']['description'],
                     channel_Subscription_Count = response['items'][0]['statistics']['subscriberCount'],
                     channel_views = response['items'][0]['statistics']['viewCount'],
                     channel_videoCount = response['items'][0]['statistics']['videoCount'],
                     channel_joined = response['items'][0]['snippet']['publishedAt'],
                     channel_Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    channel_details.append(c_details)
    return channel_details

# FUNCTION TO GET PLAYLIST DETAILS
def get_py_details(channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=50
    )
    response = request.execute()
    playlist = []
    for i in range(len(response['items'])):
        py_data = dict(py_ID = response['items'][i]['id'],
                       cl_ID = response['items'][i]['snippet']['channelId'],
                       py_name = response['items'][i]['snippet']['title'],
                       py_videocount = response['items'][i]['contentDetails']['itemCount'])
        playlist.append(py_data)
    return playlist

# FUNCTION TO GET VIDEO IDS
def get_video_ids(channel_id):
    video_ids = []
    
    # get upload id
    request = youtube.channels().list(part="contentDetails",id=channel_id).execute()
    uploads_id = request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
   
    
    v_request = youtube.playlistItems().list(
        part="contentDetails",
        maxResults=50,
        playlistId=uploads_id)
    
    while v_request:
        v_response = v_request.execute()
    
        for item in v_response.get('items',[]):
            video_ids.append(item['contentDetails']['videoId'])

        v_request = youtube.playlistItems().list_next(v_request, v_response)
    return video_ids


# Function to convert duration strings to HH:MM:SS format
def convert_duration(duration_str):
    # Remove the "PT" prefix if present
    duration_str = duration_str.replace("PT", "")
    
    # Initialize hours, minutes, and seconds to 0
    hours, minutes, seconds = 0, 0, 0
    
    # Parse the duration string to extract hours, minutes, and seconds
    if 'H' in duration_str:
        hours_str, duration_str = duration_str.split('H')
        hours = int(hours_str)
    if 'M' in duration_str:
        minutes_str, duration_str = duration_str.split('M')
        minutes = int(minutes_str)
    if 'S' in duration_str:
        seconds_str = duration_str.replace('S', '')
        seconds = int(seconds_str)
    
    # Format the duration as HH:MM:SS
    formatted_duration = f"{hours:02}:{minutes:02}:{seconds:02}"
    
    return formatted_duration

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(vo_ids):
    video_stats=[]
    for i in range(0,len(vo_ids),50):
        request = youtube.videos().list(
                  part="snippet,statistics,contentDetails",
                  id=",".join(vo_ids[i:i+50])).execute()
        
        for i in range(len(request["items"])):
            video_details=dict(channel_id = request['items'][i]['snippet']['channelId'],
                               video_id = request["items"][i]["id"], 
                               video_name = request["items"][i]["snippet"]["title"],
                               video_description = request["items"][i]["snippet"]["description"],
                               published_date = request["items"][i]["snippet"]["publishedAt"],  
                               view_count = request["items"][i]["statistics"]["viewCount"],
                               comment_count = request["items"][i]["statistics"]["commentCount"],
                               like_count = request["items"][i]["statistics"]["likeCount"],
                               duration = convert_duration(request["items"][i]["contentDetails"]["duration"]))
            
            video_stats.append(video_details)

    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comment_details(v_ids):
    comment_stats = []
    for i in v_ids:
        try:
            request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=i).execute()

            for comment in request['items']:
                comment_details = dict(Comment_id = comment['id'],
                                       Video_id = comment['snippet']['videoId'],
                                       Comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                       Comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                       Comment_publishedAt = comment['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_stats.append(comment_details)
        except:
            pass
    return comment_stats


#Main function to get all the details
def youtube_data(channel_id):
    channel = get_channel_details(channel_id)
    playlist = get_py_details(channel_id)
    vo_ids = get_video_ids(channel_id)
    video = get_video_details(vo_ids)
    comment = get_comment_details(vo_ids)
    
    final_data = {'channel details' : channel,
                      'playlist details' : playlist,
                      'video details' : video,
                      'comment details' : comment}
    return final_data


c_id = st.text_input("Enter your channel id:")

a=st.button("Extract data and store in MongoDB")

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
ch_ids = []
for i in information1.find({},{"_id":0,"channel details":1}):
     ch_ids.append(i["channel details"][0]['channel_Id'])


if c_id not in ch_ids:
    if a and c_id:
        data = youtube_data(c_id)
        information.insert_one(data)
        st.success("successfully Uploaded to MongoDB !!", icon="✅")
else:
     st.write("## :red[Already Exists]")


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in information1.find({},{"_id":0,"channel details":1}):
        ch_name.append(i["channel details"][0]['channel_name'])
    return ch_name



s=st.selectbox("Select channel",options= channel_names())
b=st.button("Migrate data from MongoDB to MySQL:")


#GET ALL DETAILS FROM MONGODB        
data1=[]
for i in information1.find({},{"_id":0,"channel details":1,"playlist details":1,"video details":1,"comment details":1}):
    if i['channel details'][0]['channel_name']==s:
      data1.append(i)


df1= pd.DataFrame(data1[0]["channel details"])
df2= pd.DataFrame(data1[0]["playlist details"])
df3= pd.DataFrame(data1[0]["video details"])
df3["duration"] = pd.to_datetime(df3["duration"])
df3["published_date"] = pd.to_datetime(df3["published_date"])
df4= pd.DataFrame(data1[0]["comment details"])
df4["Comment_publishedAt"] = pd.to_datetime(df4["Comment_publishedAt"])


cur.execute("select channel_name from channel;")
c = [i[0] for i in cur.fetchall()]


if s not in c:
    if b:
        sql1 = "insert into channel (channel_Id,channel_name,channel_description,channel_Subscription_Count,channel_views,channel_videoCount,channel_joined,channel_Playlist_Id) values (%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in range(0,len(df1)):
                cur.execute(sql1,tuple(df1.iloc[i]))
                myconnection.commit()

        sql2 = "insert into playlist (py_ID,cl_ID,py_name,py_videocount) values (%s,%s,%s,%s)"
        for i in range(0,len(df2)):
            cur.execute(sql2,tuple(df2.iloc[i]))
            myconnection.commit()

        sql3 = "insert into video (channel_id,video_id,video_name,video_description,published_date,view_count,comment_count,like_count,duration) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in range(0,len(df3)):
            cur.execute(sql3,tuple(df3.iloc[i]))
            myconnection.commit()
            
        sql4 = "insert into comment (comment_id,video_id,comment_text,Comment_author,Comment_publishedAt) values (%s,%s,%s,%s,%s)"
        for i in range(0,len(df4)):
            cur.execute(sql4,tuple(df4.iloc[i]))
            myconnection.commit()
            
        st.success("Successfully migrated to SQL !!", icon="✅")
else:
     st.write("## :red[Already Exists]")



st.write("## :green[Select any question to get Insights]")




questions = st.selectbox('Questions',
   ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?']) 
      
if questions == '1. What are the names of all the videos and their corresponding channels?':
    cur.execute("select b.video_name,a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id")
    df = pd.DataFrame(cur.fetchall())
    st.write(df)   

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cur.execute("select distinct channel_name,channel_videoCount from channel order by channel_videoCount desc")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")      
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cur.execute("select b.video_name,b.view_count,a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id order by view_count desc limit 10")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")      
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cur.execute("select video_name,comment_count from video")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cur.execute("select a.channel_name,b.video_name,b.like_count from channel as a inner join video as b on a.channel_id=b.channel_id order by like_count desc limit 10;")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,x=2,y=1,orientation='h',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cur.execute("select video_name,like_count from video")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cur.execute("select channel_name,channel_views from channel;")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cur.execute("select distinct a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id where published_date LIKE '2022%' ")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cur.execute("SELECT a.channel_name,time_format(SEC_TO_TIME(AVG(TIME_TO_SEC(b.duration))),'%H:%i:%s') AS duration FROM channel a inner join video b where b.channel_id=a.channel_id group by a.channel_name")
        df = pd.DataFrame(cur.fetchall())
        #df[1]=pd.to_datetime(df[1],format="%H$M%S.%f").datetime.hour
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cur.execute("select a.channel_name,b.video_name,b.comment_count from channel as a inner join video as b on a.channel_id=b.channel_id order by comment_count desc limit 10")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,x=1,y=2,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)    