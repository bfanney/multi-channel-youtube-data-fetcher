# -*- coding: utf-8 -*-

import sys
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
import webbrowser as wb
import json
import pandas as pd
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from bs4 import BeautifulSoup
import re
import urllib

class yt:

    def __init__(self):
        print ("Logging into all the channels...")
        
        rhvideos = self.getInstance("Red Hat Videos","rhvideos_creds.dat")
        rhvirtualization = self.getInstance("Red Hat Virtualization","rhvirtualization_creds.dat")
        rhmiddleware = self.getInstance("Red Hat Middleware","rhmiddleware_creds.dat")
        rhstorage = self.getInstance("Red Hat Storage","rhstorage_creds.dat")
        rhcloud = self.getInstance("Red Hat Cloud","rhcloud_creds.dat")
        rhlatinoamerica = self.getInstance("Red Hat Latinoamerica","rhlatinoamerica_creds.dat")
        rhapac = self.getInstance("Red Hat APAC","rhapac_creds.dat")
        rhsummit = self.getInstance("Red Hat Summit","rhsummit_creds.dat")
        rhlinux = self.getInstance("Red Hat Enterprise Linux","rhlinux_creds.dat")

        self.instances = {
            "UCp6NUFV9mSEK6RxUiEVymVg":rhvideos,
            "UCMdzdYJY7y12A367ycm-c4A":rhvirtualization,
            "UCj1LxybwM853cCvndtZmOkQ":rhmiddleware,
            "UCoyG8VyvB-XUxQl1mD3T3Gw":rhstorage,
            "UCE_2iqCm2eowadFiy2V8mUw":rhcloud,
            "UCe9KurO7bRXqRGn0756FYfA":rhlatinoamerica,
            "UCOghGALkYmQpJxj65TyWpAw":rhapac,
            "UC9CjkhQp1jX8Hbtbg6OZ9dw":rhsummit,
            "UCG5LuxhUtax6wVhH1qPNxvA":rhlinux
        }

    def selectInstance(self, c_id):
        if c_id in self.instances:
            return self.instances[c_id]
        else:
            return None

    def getInstance(self, name, token_file):
        print ("----------------------------")
        print ("Getting instance: " + name)
        print ("----------------------------")
        
        api_name = 'youtubeAnalytics'
        api_version = 'v2'
        scope = ['https://www.googleapis.com/auth/youtube.readonly']
        creds = 'client_secrets.json'
        
        client_secrets = os.path.join(os.path.dirname(creds),creds)
        flow = client.flow_from_clientsecrets(client_secrets,scope=scope,message=tools.message_if_missing(client_secrets))
        
        storage = file.Storage(token_file)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)

        return build(api_name, api_version, credentials = credentials)

    
    def execute_api_request(self, client_library_function, **kwargs):
        response = client_library_function(**kwargs).execute()
        return response

    def getVideoStats(self, yt_id, c_id, startdate, enddate):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        yt_instance = self.selectInstance(c_id)
        if yt_instance == None:
            print ("This YouTube video is not from Red Hat.")
            return None
        else:
            response = self.execute_api_request(
                yt_instance.reports().query,
                ids="channel==" + c_id,
                startDate=startdate,
                endDate=enddate,
                metrics="views,likes,dislikes,comments,shares,estimatedMinutesWatched,averageViewDuration",
                sort='-views',
                filters="video==" + yt_id,
                maxResults=200,
            )

            columns = response['columnHeaders']
            headers = []
            for i in range(len(columns)):
                headers.append(columns[i]['name'])

            table = pd.DataFrame.from_dict(response['rows'])
            table.columns = headers

            return table
    
class Channels:
    
    def scrapeChannel(self, yt_id):
        try:
            page = urllib.request.urlopen("https://www.youtube.com/watch?v=" + yt_id)
        except Exception as e:
            print (e)
            return "404"
    
        #parse the html using beautiful soup and store in variable 'soup'
        soup = BeautifulSoup(page, 'html.parser')
        #print (soup)
        File_object = open(r"soup.txt","w")
        File_object.write(str(soup))
        
        print ("---------")
        channel = soup.find(itemprop = 'channelId')
        if channel is not None:
            channel = re.search('<meta content="(.*)" itemprop="channelId"/>', str(channel)).group(1)
            return channel
        else:
            return None

    def findChannel(self, yt_id):
        self.lookup = pd.read_csv('video_dictionary.csv')
        if yt_id in self.lookup.video.values:
            print ("Found the channel ID in the library.")
            return self.lookup.loc[self.lookup['video'] == yt_id, 'channel'].iloc[0]
        else:
            print ("Scraping the channel ID")
            channel = self.scrapeChannel(yt_id)
            self.lookup = self.lookup.append({'video':yt_id, 'channel':channel} , ignore_index=True)
            self.lookup = self.lookup.set_index('video')
            self.lookup.to_csv('video_dictionary.csv')
            return channel

#Read a Drupal video export
drupal_videos = pd.read_csv('in_drupal_videos.csv')

#Instatiate YT and Channels
yt_channel = Channels()
yt = yt()

#Set start date and end date
startdate = "2017-02-28"
enddate = "2019-08-11"

#Add columns to drupal_videos
new_rows = ['views','likes','dislikes','comments','shares','estimatedMinutesWatched','averageViewDuration']
for x in new_rows:
    drupal_videos[x] = ""

for i, row in drupal_videos.iterrows():
    print ("-------------------")
    
    yt_id = row["File URL"].split("=")[1]
    print ("YouTube ID: " + yt_id)
    
    c_id = yt_channel.findChannel(yt_id)
    print ("Channel ID: " + str(c_id))

    if c_id is not None:
    
        table = yt.getVideoStats(yt_id, c_id, startdate, enddate)
        print ("Stats:")
        print (table)

        if table is not None:
            print ("Saving file...")
            drupal_videos.at[i,"views"]=table["views"].values[0]
            drupal_videos.at[i,"likes"]=table["likes"].values[0]
            drupal_videos.at[i,"dislikes"]=table["dislikes"].values[0]
            drupal_videos.at[i,"comments"]=table["comments"].values[0]
            drupal_videos.at[i,"shares"]=table["shares"].values[0]
            drupal_videos.at[i,"estimatedMinutesWatched"]=table["estimatedMinutesWatched"].values[0]
            drupal_videos.at[i,"averageViewDuration"]=table["averageViewDuration"].values[0]
            drupal_videos.to_csv("final_video_inventory.csv")

        else:
            print ("Skipping video. It's not from a Red Hat property.")

    else:
        print ("Skipping video. Can't find the channel ID.")