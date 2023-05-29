from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import pymongo
from googleapiclient import errors
import streamlit as st
import mysql.connector as mysql

st.title(':blue[YOUTUBE DATA SCRAPER]')
form=st.form('DATA SCRAPER')
channel_id=form.text_input(':violet[Enter channel_id here without quotes("")]')
form.warning('Only 10 videos will be extracted for any channel because of quota limit')
ok=form.form_submit_button('OK')

g=[]
h=[]
if ok:
    mydb=mysql.connect(user="root",password=<password>,host="localhost",database='youtube_harvest')   
    my_cursor=mydb.cursor()
    check_extract='SELECT channel_ids_list FROM channel_details'
    my_cursor.execute(check_extract)
    db8=my_cursor.fetchall()
    for i in range(len(db8)):
        if channel_id == db8[i][0]:
            g.append('1')
            st.warning('Data has been extracted for this channel_id!')
if ok and len(g)==0:
    api_service_name = "youtube"
    api_version = "v3"
    api_key=<password>
    youtube = build(api_service_name, api_version, developerKey=api_key)
    a={'channel_details':'','playlist_details':'','playlistvideoid_details':'','video_and_comment_details':''}

    #getting channel details
    l=[]
    def get_channel_info(youtube,channel_id,token=''):
        request = youtube.channels().list(part='contentDetails,snippet,statistics,status',id=channel_id,pageToken=token)
        response = request.execute()
        for i in range(len(response['items'])):   
            channel_data=dict(channel_title=response['items'][i]['snippet']['title'],
                      channel_id=response['items'][i]['id'],
                      channel_description=response['items'][i]['snippet']['description'],
                      channel_views=response['items'][i]['statistics']['viewCount'],
                      channel_subscribers=response['items'][i]['statistics']['subscriberCount'],
                      channel_videos=response['items'][i]['statistics']['videoCount'],
                      channel_status=response['items'][i]['status']['privacyStatus'])
        #a['_id']=f"{channel_data['channel_title']} {channel_data['channl_id']}"
        l.append(channel_data)
        if "nextPageToken" in response: 
            return get_channel_info(youtube, video_id,  response['nextPageToken'])
        else: 
            a['channel_details']=l
    get_channel_info(youtube,channel_id) 
    st.success('Channel Details Has Been Extracted')
    #getting playlistID using channelID
    x=[]
    o=[]
    def get_playlist_info(youtube,channel_id,token=''):
            request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            pageToken=token
        )
            response_3 = request.execute()
            for k in range(len(response_3['items'])):
                pllist_data=dict(playlist_ID=response_3['items'][k]['id'],
                                playlist_ch_id=response_3['items'][k]['snippet']['channelId'],
                                playlist_title=response_3['items'][k]['snippet']['title'],
                                playlist_videosCount=response_3['items'][k]['contentDetails']['itemCount'])
                o.append(pllist_data)
            #print(len(comments))
            if "nextPageToken" in response_3: 
                return get_playlist_info(youtube,channel_id,  response_3['nextPageToken'])
            else:
                a['playlist_details']=o
                df1=pd.DataFrame(o) 
                x.append(list(df1['playlist_ID']))
                #print(x)
    get_playlist_info(youtube,channel_id,token='') 
    st.success('Playlist Details Has Been Extracted')
    #getting videoIDs using a list of playlistIDs through looping
    p=[]
    playlist_idc=x[0]
    for i in range(len(playlist_idc)):
        playlist_id=playlist_idc[i]
        def get_playlistvideoid_info(youtube,playlist_id,token=''):
            request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,pageToken=token)
            response_4 = request.execute()
            for k in range(len(response_4['items'])):
                        videoidlist_data=dict(playlist_ID=response_4['items'][k]['id'],
                                        playlist_videoid=response_4['items'][k]['snippet']['resourceId']['videoId'])
                        p.append(videoidlist_data)
            if "nextPageToken" in response_4: 
                    return get_playlistvideoid_info(youtube,playlist_id, response_4['nextPageToken'])
            else:
                a['playlistvideoid_details']=p
        get_playlistvideoid_info(youtube,playlist_id,token='')
    v=pd.DataFrame(p)        
            
    k3=[]
    m1=[]
    v=list(v['playlist_videoid'])
    vidID_list=[]
    if len(v)>20:
      for g in range(10):
        vidID_list.append(v[g])
    else:
      for g in v:
        vidID_list.append(g)

    for g in range(len(vidID_list)):
        
        video_id=vidID_list[g]
        def get_video_info(youtube,video_id,token=''): 
            request_1 = youtube.videos().list(part='contentDetails,snippet,statistics',id=video_id,pageToken=token)
            response_1 = request_1.execute()  
            
            try: 
              for j in range(len(response_1['items'])):
                request_11 = youtube.captions().list(
                part="snippet",
                videoId=video_id)
                response_11 = request_11.execute()

                if len(response_11['items'])==0:
                  status='un_available'
                else:
                  status='available'
            except errors.HttpError: 
              status='videoNotFound'
            for j in range(len(response_1['items'])):  
              try:
                video_tags=response_1['items'][j]['snippet']['tags']
              except KeyError:
                video_tags=0
              try:
                video_category_ID=response_1['items'][j]['snippet']['categoryId'],
              except KeyError:
                video_category=0
              try:
                video_views=response_1['items'][j]['statistics']['viewCount'],
              except KeyError:
                video_views=0
              try:
                video_likeCount=response_1['items'][j]['statistics']['likeCount'],
              except KeyError:
                video_likeCount=0
              try:
                video_favoriteCount=response_1['items'][j]['statistics']['favoriteCount'], 
              except KeyError:
                video_favoriteCount=0
              try:
                video_commentCount=response_1['items'][j]['statistics']['commentCount']
              except KeyError:
                video_commentCount=0

                              
              video_data=dict(video_ID=response_1['items'][j]['id'],                            
                              video_publishedAt=response_1['items'][j]['snippet']['publishedAt'],
                              video_Ch_ID=response_1['items'][j]['snippet']['channelId'],
                                video_title=response_1['items'][j]['snippet']['title'],
                        video_thumbnails=response_1['items'][j]['snippet']['thumbnails'],
                        video_description=response_1['items'][j]['snippet']['description'],
                        video_ch_title=response_1['items'][j]['snippet']['channelTitle'],
                        video_duration=response_1['items'][j]['contentDetails']['duration'],
                        video_caption_status=status,
                        video_tags=video_tags,
                        video_category_ID=video_category_ID,
                        video_views=video_views,
                        video_likeCount=video_likeCount,
                        video_favoriteCount=video_favoriteCount, 
                        video_commentCount=video_commentCount)
              m.append(video_data)
            if "nextPageToken" in response_1: 
                #print(m)
                return get_video_info(youtube, video_id,  response_1['nextPageToken'])
            else: 
                #print(m)
                m1.append(m) 
           
            def video_comments(video_id,token=''):
              try:
                k2=[]
                # creating youtube resource object
                # retrieve youtube video results
                video_response=youtube.commentThreads().list(
                                  part='snippet,replies',
                                  videoId=video_id,pageToken=token).execute()
          
                for k in range(len(video_response['items'])):

                    # Extracting comments
                    comment_ID=video_response['items'][k]['id'],
                    comment_video_ID=video_response['items'][k]['snippet']['videoId'],
                    comment = video_response['items'][k]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_authorDisplayName=video_response['items'][k]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_publishedAt=video_response['items'][k]['snippet']['topLevelComment']['snippet']['publishedAt'],
                    # counting number of reply of comment
                    replycount = video_response['items'][k]['snippet']['totalReplyCount']
                    try:
                      # if reply is there
                      if int(replycount)>0:
                        
                        # empty list for storing reply
                        replies = []
                        # iterate through all reply
                        for k1 in range(replycount):
                          
                          # Extract reply
                          reply = video_response['items'][k]['replies']['comments'][k1]['snippet']['textOriginal']
                          
                          # Store reply is list
                          replies.append(reply)
                          
                          

                        # print comment with list of reply
                        k2.append([{'comment_ID':comment_ID,'comment_video_ID':comment_video_ID,
                                  'comment':comment,'comment_authorDisplayName':comment_authorDisplayName,
                                  'comment_publishedAt':comment_publishedAt,'replycount':replycount, 'replies':replies}])
                      
                      else:
                        
                        k2.append([{'comment_ID':comment_ID,'comment_video_ID':comment_video_ID,
                                  'comment':comment,'comment_authorDisplayName':comment_authorDisplayName,
                                  'comment_publishedAt':comment_publishedAt,'replycount':replycount, 'replies':0}])
                      
                    except IndexError:
                        k2.append([{'comment_ID':comment_ID,'comment_video_ID':comment_video_ID,
                                  'comment':comment,'comment_authorDisplayName':comment_authorDisplayName,
                                  'comment_publishedAt':comment_publishedAt,'replycount':replycount, 'replies':0}])
                # Again repeat
                if 'nextPageToken' in video_response:
                  return video_comments(video_id,  video_response['nextPageToken'])
                  #print(k2)
                else:
                  
                  k3.append(k2)
               except errors.HttpError:
                  k2.append([{'comment_ID':np.nan,'comment_video_ID':np.nan,
                            'comment':np.nan,'comment_authorDisplayName':np.nan,
                            'comment_publishedAt':np.nan,'replycount':np.nan, 'replies':np.nan}])
                  k3.append(k2)
            # Enter video id
            video_id = vidID_list[g]
            # Call function
            video_comments(video_id,token='')
        m=[]
        get_video_info(youtube,video_id,token='') 
    #print(len(k3))
    #print(len(m1))
    z=[]
    for n in range(len(m1)):
        y={f'video{n+1}':m1[n],
              f'video{n+1}_comments':k3[n]}
        z.append(y)
    a['video_and_comment_details']=z
    client = pymongo.MongoClient("mongodb+srv://KPKAVIN:kasaan@cluster0.3bc5s5h.mongodb.net/?retryWrites=true&w=majority")
    db=client.harvest
    records=db.har_col
    records.insert_one(a)
    st.success('Video Details Has Been Extracted')
    st.success(f'All Details of channel_id {channel_id} has been uploaded to the mongodb database')
    #EXTRACTING DATA FROM MONGODB TO MYSQL

    #client = pymongo.MongoClient("mongodb+srv://KPKAVIN:<password>@cluster0.3bc5s5h.mongodb.net/?retryWrites=true&w=majority")
    #db=client.harvest
    #records=db.har_col

    #extracting channel details
    channel_ids_list=[]
    channel_title_list=[]
    channel_description_list=[]
    channel_views_list=[]
    channel_subscribers_list=[]
    channel_videos_list=[]
    channel_status_list=[]
    #channel_id='UC1Da-IploOtTuUGbIUX4MHg'
    #print(list(x))
    r=['channel_id','channel_title','channel_description','channel_views','channel_subscribers','channel_videos','channel_status']
    #final list
    r1=[channel_ids_list,channel_title_list,channel_description_list,channel_views_list,channel_subscribers_list,
        channel_videos_list,channel_status_list]
    for c in range(len(r1)):
        x=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'channel_details.{r[c]}':1})
        for i in x:
            h=(list(i.values()))
            for j in h:
                for k in j:
                    r1[c].append(''.join(list(k.values())))

    #Connecting to mysql Database
    mydb=mysql.connect(user="root",password=<password>,host="localhost",database='youtube_harvest')   
    my_cursor=mydb.cursor()
    for w in range(len(channel_ids_list)):  
        query=f"INSERT INTO channel_details (channel_ids_list,channel_title_list,channel_description_list,channel_views_list,channel_subscribers_list,channel_videos_list,channel_status_list ) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        records_ch=(channel_ids_list[w],channel_title_list[w],channel_description_list[w],channel_views_list[w],channel_subscribers_list[w],channel_videos_list[w],channel_status_list[w])
        my_cursor.execute(query,records_ch)
        mydb.commit()

    def array_rem_1(l):
        for i in range(len(l)):
            x=(int(l[i][1:-1]))
            l[i]=x
            

                
    #extracting playlist details
    playlist_ids_list=[]
    playlist_ch_ids_list=[]
    playlist_title_list=[]
    playlist_videocount_list=[]
    p=['playlist_ID','playlist_ch_id','playlist_title','playlist_videosCount']
    #final list
    p1=[playlist_ids_list,playlist_ch_ids_list,playlist_title_list,playlist_videocount_list]

    for c1 in range(len(p1)):
        x1=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'playlist_details.{p[c1]}':1})
        for i1 in x1:
            h1=(list(i1.values()))
            for j1 in h1:
                try:
                    for k1 in j1:
                        p1[c1].append(''.join(list(k1.values())))
                except TypeError:
                    for k1 in j1:
                        p1[c1].append(''.join(str(list(k1.values()))))

    array_rem_1(playlist_videocount_list)

    for w1 in range(len(playlist_ids_list)):  
        
        query=f"INSERT INTO playlist_details (playlist_ids_list,playlist_ch_ids_list,playlist_title_list,playlist_videocount_list ) VALUES (%s,%s,%s,%s)"
        records_pl=(playlist_ids_list[w1],playlist_ch_ids_list[w1],playlist_title_list[w1],playlist_videocount_list[w1])
        my_cursor.execute(query,records_pl)
        mydb.commit()



    #extracting playlist items
    playlistitems_ids_list=[] #same as playlist id
    playlistitems_video_ids_list=[]
    pl=['playlist_ID','playlist_videoid']
    #final combination of lists containing playlist details
    pl1=[playlistitems_ids_list,playlistitems_video_ids_list]  
    for c2 in range(len(pl1)):
        x2=records.find({},{'_id':0,f'playlistvideoid_details.{pl[c2]}':1})
        for i2 in x2:
            h2=(list(i2.values()))
            for j2 in h2:
                try:
                    for k2 in j2:
                        pl1[c2].append(''.join(list(k2.values())))
                except TypeError:
                    for k2 in j2:
                        pl1[c2].append(''.join(str(list(k2.values()))))  
                    
      
    print(playlistitems_video_ids_list)

    #client = pymongo.MongoClient("mongodb+srv://KPKAVIN:<password>@cluster0.3bc5s5h.mongodb.net/?retryWrites=true&w=majority")
    #db=client.harvest
    #records=db.har_col
    def extract(y,a,b):
        for  j in y:
                #for t in j:
                    #h=(['video_and_comment_details'][0][f'video{l}'][0]['video_ID'])
                try:
                    for t in (list(j.values())):
                        a.append(t[n][f'video{l}'][0][b])
                except IndexError:
                    a.append(np.nan)
                    
    def extract_2(y,a,b,c):
        for  j in y:
                #for t in j:
                    #h=(['video_and_comment_details'][0][f'video{l}'][0]['video_ID'])
                try:
                    for t in (list(j.values())):
                        a.append(t[n][f'video{l}'][0][b][c]['url'])
                except IndexError:
                    a.append(np.nan)
                    
    def extract_3(y,a,b):
        for  j in y:
                #for t in j:
                    #h=(['video_and_comment_details'][0][f'video{l}'][0]['video_ID'])
                try:
                    for t in (list(j.values())):
                        a.append(t[n][f'video{l}'][0][b])
                except IndexError:
                    a.append(np.nan)
                    


    e=[]
    e1=[]
    e2=[]
    e_2=[]
    e3=[]
    e4=[]
    e5=[]
    e6=[]
    e7=[]
    e8=[]
    e9=[]
    e10=[]
    e11=[]
    e12=[]
    e13=[]
    l=1
    n=0
    #limiting Extractions
    while l<100:
        x4=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_ID':1})
        extract(x4,e,'video_ID')

        x5=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_Ch_ID':1})
        extract(x5,e1,'video_Ch_ID')
        
        x6=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_publishedAt':1})
        extract(x6,e2, 'video_publishedAt')

        x_6=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_title':1})
        extract(x_6,e_2, 'video_title')
        
        x7=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_description':1})
        extract(x7,e3,'video_description')
        
        x8=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_ch_title':1})
        extract(x8,e4,'video_ch_title')
        
        x9=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_duration':1})
        extract(x9,e5,'video_duration')
        
        x_10=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_caption_status':1})
        extract(x_10,e6,'video_caption_status')
        

        
        x_11=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_tags':1})
        extract(x_11,e7,'video_tags')
        
        x_12=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_category_ID':1})
        extract(x_12,e8,'video_category_ID')
        
        x_13=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_views':1})
        extract(x_13,e9,'video_views')
        
        x_14=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_likeCount':1})
        extract_3(x_14,e10,'video_likeCount')
        
        x_15=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_favoriteCount':1})
        extract_3(x_15,e11,'video_favoriteCount')
        
        x_16=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_commentCount':1})
        extract(x_16,e12,'video_commentCount')
        
        x_161=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}.video_thumbnails.default':1})
        extract_2(x_161,e13,'video_thumbnails','default')
        
        n+=1    
        l+=1
    df=pd.DataFrame({'video_ID':e,'video_Ch_ID':e1,'video_publishedAt':e2,'video_title':e_2,'video_description':e3,'video_ch_title':e4,
                    'video_duration':e5,'video_caption_status':e6,'video_tags':e7,'video_category_ID':e8,'video_views':e9,
                    'video_likeCount':e10,'video_favoriteCount':e11,'video_commentCount':e12,'video_thumbnails_default':e13})
    df1=df.dropna().reset_index(drop=True)

    def array_rem(l):
        for i in range(len(l)):
            try:
                if l[i][0]==str(l[i][0]):
                    l[i]=(int(l[i][0]))
            except TypeError:
                    l[i]=0
    a1=list(df1['video_ID'])
    a2=list(df1['video_Ch_ID'])
    a3=list(df1['video_publishedAt'])
    a_3=list(df1['video_title'])
    a4=list(df1['video_description'])
    a5=list(df1['video_ch_title'])
    a6=list(df1['video_duration'])

    a7=list(df1['video_caption_status'])
    a81=list(df1['video_tags'])
    a8=[]
    for i in a81:
        if i==0:
            a8.append(['no_tags'])
        else:
            a8.append(i)

    a9=list(df1['video_category_ID'])
    array_rem(a9)
    a10=list(df1['video_views'])
    array_rem(a10)
    a11=list(df1['video_likeCount'])
    array_rem(a11)
    a12=list(df1['video_favoriteCount'])
    array_rem(a12)
    a13=list(df1['video_commentCount'])
    a14=list(df1['video_thumbnails_default'])

    for w3 in range(len(a1)):  
       
        query=f"INSERT INTO video_details(video_ID,video_Ch_ID ,video_publishedAt ,video_title,video_description,video_ch_title ,video_duration ,video_caption_status,video_tags,video_category_ID ,video_views ,video_likeCount ,video_favoriteCount ,video_commentCount ,video_thumbnails_default ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        records_pli=(a1[w3],a2[w3],a3[w3],a_3[w3],a4[w3],a5[w3],a6[w3],a7[w3],a8[w3][0],a9[w3],a10[w3],a11[w3],a12[w3],a13[w3],a14[w3])
        my_cursor.execute(query,records_pli)
        mydb.commit()

    d=records.find_one()
    l=1
    n=0

    def array_rem_2(l):
        for i in range(len(l)):
            if l[i]=='videoNotFound':
                l[i]='no_data'
            else:
                x=l[i][0]
                l[i]=x

    def array_rem_3(l):
        for i in range(len(l)):
            if l[i]=='videoNotFound':
                l[i]='no_data'
            else:
                pass
            

            
    ec=[]
    ec1=[]
    ec2=[]
    ec3=[]
    ec4=[]
    ec5=[]
    ec6=[]
    while l<100:
        def extract_1(y,a,b):
            for  j in y:
                    #for t in j:
                        #h=(['video_and_comment_details'][0][f'video{l}'][0]['video_ID'])
                    try:
                        for t in (list(j.values())):
                            a.append(t[n][f'video{l}_comments'][0][0][b])
                    except IndexError:
                        a.append(np.nan)

        x_162=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.comment_ID':1})
        extract_1(x_162,ec,'comment_ID')
        
        x_17=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.comment_video_ID':1})
        extract_1(x_17,ec1,'comment_video_ID')
        
        x_18=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.comment':1})
        extract_1(x_18,ec2,'comment')
        
        x_19=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.comment_authorDisplayName':1})
        extract_1(x_19,ec3,'comment_authorDisplayName')
        
        x_19=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.comment_publishedAt':1})
        extract_1(x_19,ec4,'comment_publishedAt')
        
        x_20=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.replycount':1})
        extract_1(x_20,ec5,'replycount')
        
        x_21=records.find({'channel_details.channel_id':f"{channel_id}"},{'_id':0,f'video_and_comment_details.video{l}_comments.replies':1})
        extract_1(x_21,ec6,'replies')
        
        l+=1
        n+=1
    df2=pd.DataFrame({'comment_ch_ID':ec,'comment_video_ID':ec1,'comment':ec2,'comment_authorDisplayName':ec3,'comment_publishedAt':ec4,
                      'comments_replycount':ec5,'comments_replies':ec6})
    df3=df2.dropna().reset_index(drop=True)
    #print(df3)
    array_rem_2(list(df3['comment_ch_ID']))
    array_rem_2(list(df3['comment_video_ID']))
    array_rem_2(list(df3['comment']))
    array_rem_2(list(df3['comment_authorDisplayName']))
    array_rem_2(list(df3['comment_publishedAt']))
    array_rem_3(list(df3['comments_replycount']))
    list(df3['comments_replies'])
    for u in range(len(list(df3['comments_replies']))):
        if list(df3['comments_replies'])[u]==0:
            list(df3['comments_replies'])[u]=['no_reply']

    for w4 in range(len(list(df3['comment_ch_ID']))):  
        query=f"INSERT INTO comment_details(comment_ch_ID,comment_video_ID,comment,comment_authorDisplayName,comment_publishedAt,comments_replycount,comments_replies ) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        records_plc=(list(df3['comment_ch_ID'])[w4][0],list(df3['comment_video_ID'])[w4][0],list(df3['comment'])[w4][0],
                    list(df3['comment_authorDisplayName'])[w4][0],list(df3['comment_publishedAt'])[w4][0],
                    list(df3['comments_replycount'])[w4],
                    str(list(df3['comments_replies'])[w4]))
        my_cursor.execute(query,records_plc)
        mydb.commit() 
        
    st.success(f'All Details of channel_id {channel_id} has been warehoused in the MYSQl database')

ch_l=['Select']
mydb=mysql.connect(user="root",password=<password>,host="localhost",database='youtube_harvest')   
my_cursor=mydb.cursor()
y2='SELECT channel_title_list FROM channel_details'
my_cursor.execute(y2)
db1=my_cursor.fetchall()
for i in range(len(db1)):
    ch_l.append(db1[i][0])
show_details=st.selectbox('Do you want to see the extracted details of any channels',(ch_l))

#print(t)
y3=f'SELECT * FROM channel_details WHERE channel_title_list = "{show_details}"'
my_cursor.execute(y3)
db2=my_cursor.fetchall()

ch_l1={'channel_id':'','channel_views':'','channel_subscribers':'','channel_videos':'','channel_status':''}
ch_desc=[]
for i in db2:
    ch_l1['channel_id']=db2[0][0]
    ch_desc.append(db2[0][2])
    ch_l1['channel_views']=db2[0][3]
    ch_l1['channel_subscribers']=db2[0][4]
    ch_l1['channel_videos']=db2[0][5]
    ch_l1['channel_status']=db2[0][6]

if ch_l1['channel_id']!='':
    df=(pd.DataFrame(ch_l1,index=[k for k in range(1)]))
    st.subheader('Channel Details')
    st.table(df)
    if len(ch_desc)>0:
        st.subheader('Channel Description')
        st.write(ch_desc[0])
#Display video details
y4=f'SELECT * FROM video_details WHERE video_ch_title= "{show_details}"'
my_cursor.execute(y4)
db3=my_cursor.fetchall()

vi_l1={'video_id':'','video_Ch_id':'','video_publishedAt':'','video_title':'','video_description':'','video_duration':'','video_caption_status':'',
       'video_tags':'','video_category_id':'','video_views':'','video_likecount':'','video_likecount':'','video_favoritecount':'',
       'video_commentcount':'','video_thumbnails_default':''}
vi_desc=[]
n=[]
n1=[]
n2=[]
n3=[]
n4=[]
n5=[]
n6=[]
n7=[]
n8=[]
n9=[]
n10=[]
n11=[]
n12=[]
n13=[]
n14=[]
for m in range(len(db3)):
    n.append(db3[m][0])
    n1.append(db3[m][1])
    n2.append(db3[m][2])
    n3.append(db3[m][3])
    n4.append(db3[m][4])
    n5.append(db3[m][6])
    n6.append(db3[m][7])
    n7.append(db3[m][8])
    n8.append(db3[m][9])
    n9.append(db3[m][10])
    n10.append(db3[m][11])
    n11.append(db3[m][12])
    n12.append(db3[m][13])
    n13.append(db3[m][14])
    
   

vi_l1['video_id']=n
vi_l1['video_Ch_id']=n1
vi_l1['video_publishedAt']=n2
vi_l1['video_description']=n4
vi_l1['video_title']=n3
vi_l1['video_duration']=n5
vi_l1['video_caption_status']=n6
vi_l1['video_tags']=n7
vi_l1['video_category_id']=n8
vi_l1['video_views']=n9
vi_l1['video_likecount']=n10
vi_l1['video_favoritecount']=n11
vi_l1['video_commentcount']=n12
vi_l1['video_thumbnails_default']=n13


if vi_l1['video_id']!='':
    df=(pd.DataFrame(vi_l1,index=[k for k in range(len(n6))]))
    st.subheader(f'{show_details} Video Details')
    st.table(df)
    
st.title('Few SQL Queries')
q=st.checkbox('Click on this checkbox to view names of all the videos and their corresponding channels')
def query(h,i,y5,g,g1):
    h=[]
    i=[]
    my_cursor.execute(y5)
    db4=my_cursor.fetchall()
    for z in range(len(db4)):
        h.append(db4[z][0])
        i.append(db4[z][1])
    o=pd.DataFrame({f'{g}':i,f'{g1}':h},index=[l for l in range(1,len(i)+1)])
    st.table(o)
    
def query_1(y6,g2,g3):
    f=[]
    e=[]
    #y6=f'SELECT video_title,video_commentCount FROM video_details'
    my_cursor.execute(y6)
    db4=my_cursor.fetchall()
    for z in range(len(db4)):
        f.append(db4[z][0])
        e.append(db4[z][1])
    o2=pd.DataFrame({f'{g2}':f,f'{g3}':e},index=[l for l in range(1,len(e)+1)])
    df = o2.astype({f'{g2}':'string',f'{g3}':'int'})
    st.table(df.sort_values(by=[f'{g3}'], ascending=False))
    
def query_2(y7,g4,g5,g6):
    f=[]
    e=[]
    g=[]
    my_cursor.execute(y7)
    db4=my_cursor.fetchall()
    for z in range(len(db4)):
        f.append(db4[z][0])
        e.append(db4[z][1])
        g.append(db4[z][2])
    o2=pd.DataFrame({f'{g4}':e,f'{g5}':g,f'{g6}':f},index=[l for l in range(1,len(e)+1)])
    st.table(o2)
if q:
    y5=f'SELECT video_Ch_ID,video_title FROM video_details'
    query(h,i,y5,'Video_title','Video_channel_ID')
q1=st.checkbox('Click on this checkbox to view the channels that have the most number of videos, and how many videos do they have')
if q1:
    y5=f'SELECT channel_title_list,channel_videos_list FROM channel_details ORDER BY channel_videos_list DESC'
    query(h,i,y5,'Channel_videos_count','Channel_title')
q2=st.checkbox('Click on this checkbox to view the top 10 most viewed videos and their respective channels')
if q2:
    y7=f'SELECT video_Ch_title,video_title,video_views FROM video_details ORDER BY video_views DESC LIMIT 10'
    query_2(y7,'Video_title','Video_views','Video_channel_ID')

q3=st.checkbox('Click on this checkbox to view how many comments were made on each video, and what are their corresponding video names')
if q3:
  y6=f'SELECT video_title,video_commentCount FROM video_details'
  query_1(y6,'Video_title','Video_commentCount')
q4=st.checkbox('Click on this checkbox to view the videos that have the highest number of likes, and what are their corresponding channel names')
if q4:
    y7=f'SELECT video_Ch_title,video_title,video_likeCount FROM video_details ORDER BY video_likeCount DESC'
    query_2(y7,'Video_title','Video_likeCount','Video_Channel_title')
     
q5=st.checkbox('Click on this checkbox to view the channels that have the most number of views, and their corresponding channel names')
if q5:
    y5=f'SELECT channel_title_list,channel_views_list FROM channel_details ORDER BY channel_views_list DESC'
    query(h,i,y5,'Channel_views_count','Channel_title')
    
q6=st.checkbox('Click on this checkbox to view Which videos have the highest number of comments, and what are their corresponding channel names')
if q6:
    y6=f'SELECT video_Ch_title,video_commentCount FROM video_details'
    query_1(y6,'video_Ch_title','video_commentCount')
    
q7=st.checkbox('Click on this checkbox to view the names of all the channels that have published videos in the year 2023')
if q7:
    y5=f'SELECT video_Ch_title,video_title,video_publishedAt FROM video_details'
    h=[]
    i=[]
    j=[]
    my_cursor.execute(y5)
    db4=my_cursor.fetchall()
    for z in range(len(db4)):
        h.append(db4[z][0])
        i.append(db4[z][1])
        j.append(db4[z][2])
    o=pd.DataFrame({'video_publishedAt':j,'video_title':i,'video_Ch_title':h},index=[l for l in range(1,len(i)+1)])
    df=o.astype({'video_publishedAt':'datetime64[ns]','video_title':'str','video_Ch_title':'str'})
    st.table(df[(df['video_publishedAt'] >"2023-01-01") & (df['video_publishedAt'] < "2023-12-31")].sort_values(by='video_publishedAt'))
    
q8=st.checkbox('Click on this checkbox to view the average duration of all videos in each channel, and what are their corresponding channel names')
if q8:
    y5=f'SELECT video_Ch_title,video_duration FROM video_details'
    h=[]
    i=[]
    
    my_cursor.execute(y5)
    db4=my_cursor.fetchall()
    for z in range(len(db4)):
        h.append(db4[z][0])
        i.append(db4[z][1])
        
    o=pd.DataFrame({'video_Ch_title':h,'video_duration':i},index=[l for l in range(1,len(i)+1)])
    def time(x):
        h=0
        
        s=x.strip('PT')
        for g in range(len(s)):
            if s[g]=='H':
                h+=int(s[:(g)])*60*60
        s1=x.strip('PT')
        for g1 in range(len(s1)):
            try:
                if s1[g1]=='M':
                    f=s1.index('H')
                    j=int(''.join(s1[f+1:g1]))*60
                    h+=j
            except ValueError:
                    if s1[g1]=='M':
                        h+=int(s1[:(g1)])*60
        for g1 in range(len(s1)):
            try:
                if s1[g1]=='S':
                    f=s1.index('M')
                    j=int(''.join(s1[f+1:g1]))
                    h+=j
            except ValueError:
                    if s1[g1]=='H':
                        t=s1.index('H')
                        t1=s1.index('S')
                        h+=int(s1[t+1:(t1)])
                 
        return h
            
        
    df2=list(o['video_duration'].apply(lambda x:time(x)))
    o1=pd.DataFrame({'video_Ch_title':h,'Average duration_in_mins':df2},index=[l for l in range(1,len(i)+1)]).groupby(['video_Ch_title']).mean()/60
    st.table(o1)
