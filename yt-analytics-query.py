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
from datetime import date, datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pafy

#This class logs into YouTube and collects stats via the Analytics API
class YouTube:

    def __init__(self):
        print ("Logging into all the channels...")
        
        #Login to all the Red Hat channels
        rhvideos = self.getInstance("Red Hat Videos","client_secrets.json","rhvideos_creds.dat")
        rhvirtualization = self.getInstance("Red Hat Virtualization","client_secrets.json","rhvirtualization_creds.dat")
        rhmiddleware = self.getInstance("Red Hat Middleware","client_secrets.json","rhmiddleware_creds.dat")
        rhstorage = self.getInstance("Red Hat Storage","client_secrets.json","rhstorage_creds.dat")
        rhcloud = self.getInstance("Red Hat Cloud","client_secrets.json","rhcloud_creds.dat")
        rhlatinoamerica = self.getInstance("Red Hat Latinoamerica","client_secrets.json","rhlatinoamerica_creds.dat")
        rhapac = self.getInstance("Red Hat APAC","client_secrets.json","rhapac_creds.dat")
        rhsummit = self.getInstance("Red Hat Summit","client_secrets.json","rhsummit_creds.dat")
        rhlinux = self.getInstance("Red Hat Enterprise Linux","client_secrets.json","rhlinux_creds.dat")
        rhemea = self.getInstance("Red Hat EMEA","client_secrets_emea.json","rhemea_creds.dat")

        #Get the special Red Hat EMEA instance


        #Save the instances in a dictionary, so the correct one can be selected based on the channel ID
        self.instances = {
            "UCp6NUFV9mSEK6RxUiEVymVg":rhvideos,
            "UCMdzdYJY7y12A367ycm-c4A":rhvirtualization,
            "UCj1LxybwM853cCvndtZmOkQ":rhmiddleware,
            "UCoyG8VyvB-XUxQl1mD3T3Gw":rhstorage,
            "UCE_2iqCm2eowadFiy2V8mUw":rhcloud,
            "UCe9KurO7bRXqRGn0756FYfA":rhlatinoamerica,
            "UCOghGALkYmQpJxj65TyWpAw":rhapac,
            "UC9CjkhQp1jX8Hbtbg6OZ9dw":rhsummit,
            "UCG5LuxhUtax6wVhH1qPNxvA":rhlinux,
            "UCBwSCyzT3GukpVdUdxMjKYw":rhemea
        }
    
    #This method logs into a YouTube account and returns an instance
    def getInstance(self, name, creds, token_file):
        print ("----------------------------")
        print ("Getting instance: " + name)
        print ("----------------------------")
        
        api_name = 'youtubeAnalytics'
        api_version = 'v2'
        scope = ['https://www.googleapis.com/auth/youtube.readonly']
        
        client_secrets = os.path.join(os.path.dirname(creds),creds)
        flow = client.flow_from_clientsecrets(client_secrets,scope=scope,message=tools.message_if_missing(client_secrets))
        
        storage = file.Storage(token_file)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)

        return build(api_name, api_version, credentials = credentials)

    #This method selects the right YouTube instance based on the channel ID
    def selectInstance(self, c_id):
        if c_id in self.instances:
            return self.instances[c_id]
        else:
            return None

    #This method grabs stats from a YouTube video using the Analytics API. It calls the correct YouTube instance using the
    #selectInstance method. It returns a Dataframe with the data requested. Thank you @grountre.
    def getVideoStats(self, yt_id, c_id, startdate, enddate):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        yt_instance = self.selectInstance(c_id)
        if yt_instance == None:
            print ("This YouTube video is not from Red Hat.")
            return None
        else:
            #Get overall stats
            response = self.executeAPIRequest(
                yt_instance.reports().query,
                ids="channel==" + c_id,
                startDate=startdate,
                endDate=enddate,
                metrics="views,likes,dislikes,comments,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
                sort='-views',
                filters="video==" + yt_id,
                maxResults=10,
            )

            columns = response['columnHeaders']
            headers = []
            for i in range(len(columns)):
                headers.append(columns[i]['name'])

            table = pd.DataFrame.from_dict(response['rows'])
            table.columns = headers
            
            #add redhat.com embedded views here
            response2 = self.executeAPIRequest(
                yt_instance.reports().query,
                ids="channel==" + c_id,
                startDate=startdate,
                endDate=enddate,
                metrics="views,estimatedMinutesWatched",
                sort='-views',
                filters="video==" + yt_id + ";insightPlaybackLocationType==EMBEDDED",
                dimensions="insightPlaybackLocationDetail",
                maxResults=20,
            )

            rhviews=0
            rhminuteswatched=""
            for row in response2['rows']:
                if row[0]=="redhat.com":
                    rhviews=row[1]
                    rhminuteswatched=row[2]

            #get video length
            url = "http://www.youtube.com/watch?v=" + yt_id
            video = pafy.new(url)

            #pass back a single table
            table["redhat.com views"]=rhviews
            table["redhat.com minutes watched"]=rhminuteswatched
            table["video length"]=video.length

            return table

    #This method executes the API request. Thank you @grountre.
    def executeAPIRequest(self, client_library_function, **kwargs):
        response = client_library_function(**kwargs).execute()
        return response

#This class identifies the correct YouTube channel ID per a given YouTube video ID
class Channels:
    
    #This method scrapes a YouTube video webpage to find the channel ID in the HTML metadata. It saves the
    #channel ID to a video_dictionary.csv so it isn't necessary to scrape YouTube for a past hit.
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

    #This method attempts to lookup a channel ID in video_dictionary.csv. If it doesn't exist, it scrapes 
    #YouTube to get the information using the scrapeChannel method.
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

class Sheets:

    def __init__(self, creds, docid, worksheet_name):
        self.docid = docid
        self.worksheet_name = worksheet_name
        
        print ("Logging into Google.")
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(creds, scope)
        client = gspread.authorize(credentials)
        sh = client.open_by_key(self.docid)
        self.worksheet = sh.worksheet(self.worksheet_name)

    def setGoogleSheet(self, data):
        print ("Uploading Google Sheet.")
        set_with_dataframe(self.worksheet, data)

    def blankGoogleSheet(self):
        print ("Blanking Google Sheet.")
        sheet = self.worksheet.get_all_values()

        headers = sheet.pop(0)
        sheet = pd.DataFrame(sheet, columns=headers)

        length = len(sheet.index)+1

        cells = "A2:AG" + str(length)

        cell_list = self.worksheet.range(cells)
        for cell in cell_list:
            cell.value = ""

        self.worksheet.update_cells(cell_list)

#Read a Drupal video export
drupal_videos = pd.read_csv('in_drupal_videos.csv')

#Instatiate YT and Channels
yt_channel = Channels()
YouTube = YouTube()

#Set start date and end date
startdate = "2017-02-28"

enddate = datetime.today()
enddate = enddate.strftime("%Y-%m-%d")

#enddate = "2019-08-20"

#Loop through the drupal Videos export to get stats
for i, row in drupal_videos.iterrows():
    print ("-------------------")
    
    print ("Row: " + str(i))

    yt_id = row["File URL"].split("=")[1]
    print ("YouTube ID: " + yt_id)
    
    c_id = yt_channel.findChannel(yt_id)
    print ("Channel ID: " + str(c_id))

    #Error checking: Ensure a channel ID is returned
    if c_id is not None:
    
        error_count = 0

        try:
            table = YouTube.getVideoStats(yt_id, c_id, startdate, enddate)
        except Exception as e:
            while error_count <= 5:
                print ("Error getting video stats. Trying again.")
                table = YouTube.getVideoStats(yt_id, c_id, startdate, enddate)
                error_count += 1
            if error_count >= 6:
                print ("The script couldn't get video stats after 5 tries. Quitting now.")
                sys.exit(0)                

        #Error checking: Ensure YouTube data is returned
        if table is not None:
            table.replace('',0, inplace=True)
            print ("Stats:")
            print (table)
            print ("Saving file...")
            drupal_videos.at[i,"Views"]=table["views"].values[0]
            drupal_videos.at[i,"redhat.com views"]=table["redhat.com views"].values[0]
            drupal_videos.at[i,"Likes"]=table["likes"].values[0]
            drupal_videos.at[i,"Dislikes"]=table["dislikes"].values[0]
            drupal_videos.at[i,"Comments"]=table["comments"].values[0]
            drupal_videos.at[i,"Shares"]=table["shares"].values[0]
            drupal_videos.at[i,"Minutes watched"]=table["estimatedMinutesWatched"].values[0]
            drupal_videos.at[i,"Video length"]=table["video length"].values[0]

            print ("Red Hat minutes watched: ")
            print (int(table["redhat.com minutes watched"].values[0]))

            drupal_videos.at[i,"redhat.com minutes watched"]=int(table["redhat.com minutes watched"].values[0])
            drupal_videos.at[i,"Average view duration"]=table["averageViewDuration"].values[0]
            drupal_videos.at[i,"Average view percentage"]=(float(table["averageViewPercentage"].values[0])/100)
           
            #reorganize columns
            drupal_videos=drupal_videos[['Views','Likes','Dislikes','Comments','Shares','Minutes watched',
            'Average view duration','Average view percentage','Video length','redhat.com views','redhat.com minutes watched','Title','Meta Date','Channel',
            'Success story type','Product','Product line','Solution','Services','Industry','Topic','Business challenge','Partners',
            'Region','Featured Groupings','Offer ID','Original Author','Revision Author','Published','Updated Date','Language','File URL','Node ID']]
            
            #write file to disk
            drupal_videos.to_csv("final_video_inventory.csv")

        else:
            print ("Skipping video. It's not from a Red Hat property.")

    else:
        print ("Skipping video. Can't find the channel ID.")

#Upload the final sheet to the inventory
gsheets = Sheets("gsheet_creds.json","1PqPptUrQvRqWExXnCe-iPMAdyyCuhs_BTvYH2Gv9YGw","Videos")
gsheets.blankGoogleSheet()
gsheets.setGoogleSheet(drupal_videos)