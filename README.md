# YouTube-Data-Harvesting-and-Warehousing:
## Click Here to view demo https://drive.google.com/file/d/1Meof5onfYo5ujad9DktpeLA_Kf1yS8Xt/view?usp=sharing

## Some Screenshots of the app:
![Screenshot (216)](https://github.com/Kav1n-Lal/YouTube-Data-Harvesting-and-Warehousing-/assets/116146011/4fef54b1-de43-4f0a-90c7-fff0c882a4ab)
![Screenshot (217)](https://github.com/Kav1n-Lal/YouTube-Data-Harvesting-and-Warehousing-/assets/116146011/814e1e69-74b8-497e-ae55-430ce6f004f9)
![Screenshot (218)](https://github.com/Kav1n-Lal/YouTube-Data-Harvesting-and-Warehousing-/assets/116146011/098111f9-c863-446c-859f-6642c1627f04)
![Screenshot (219)](https://github.com/Kav1n-Lal/YouTube-Data-Harvesting-and-Warehousing-/assets/116146011/7ae0d7fb-928c-4861-bef7-bb6368f69793)
![Screenshot (220)](https://github.com/Kav1n-Lal/YouTube-Data-Harvesting-and-Warehousing-/assets/116146011/a393021b-53d1-4a93-b3af-dcad8a5a335a)

**Create a database in MYSQL and create these tables below:**

**CREATE TABLE channel_details(
channel_ids_list varchar(255) primary key,
channel_title_list varchar(255),channel_description_list text,
channel_views_list int,channel_subscribers_list int,
    channel_videos_list int,channel_status_list varchar(255)
    );**

**CREATE TABLE playlist_details(
playlist_ids_list varchar(255) primary key,playlist_ch_ids_list varchar(255),
playlist_title_list varchar(1000),playlist_videocount_list int,
foreign key (playlist_ch_ids_list) references channel_details(channel_ids_list)
);**

**CREATE TABLE video_details(
video_ID varchar(255) primary key,video_Ch_ID varchar(255),video_publishedAt varchar(255),
video_title varchar(1000),video_description text,
video_ch_title varchar(1000),
 video_duration varchar(100),video_caption_status varchar(255),video_tags varchar(255),
 video_category_ID int,video_views int,
 video_likeCount int,video_favoriteCount int,video_commentCount varchar(255),
 video_thumbnails_default varchar(255),
foreign key (video_Ch_ID) references channel_details(channel_ids_list)
);**

**CREATE TABLE comment_details(
comment_ch_ID varchar(255) ,comment_video_ID varchar(255),
comment text,comment_authorDisplayName varchar(255),comment_publishedAt varchar(255),
comments_replycount varchar(255),comments_replies varchar(1000),
foreign key (comment_video_ID) references video_details(video_ID)
);**

**Run the youtube_harvest.py file in VS Code**

**On Line 30 Enter your youtube api key**
**On Lines 18,295,602-Enter your SQL database password**
**On Lines 268,363-Enter your Mongodb database password**
