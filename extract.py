{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyMTEDl9gUJEuJGb5qspO84E",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/tyeborg/mean-tweet-pipeline/blob/master/extract.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "qb9K7N1QMvw6"
      },
      "outputs": [],
      "source": []
    },
    {
      "cell_type": "code",
      "source": [
        "import pandas as pd\n",
        "from dateutil import parser\n",
        "from googleapiclient.discovery import build\n",
        "\n",
        "class Extract():\n",
        "    def _init_(self):\n",
        "        # Initialize variables.\n",
        "        self.api_service_name = 'youtube'\n",
        "        self.api_version = 'v3'\n",
        "        self.api_key = 'AIzaSyBfsz13OHhGI51aPMASG4NtfmM4EBbWyDY'\n",
        "        self.channel_id = 'UCZGYJFUizSax-yElQaFDp5Q' @starwars channel id\n",
        "        self.youtube = build(self.api_service_name, self.api_version, developerKey=self.api_key)\n",
        "    \n",
        "    def get_channel_info(self):\n",
        "        # Get the channel information\n",
        "        channel_response = self.youtube.channels().list(\n",
        "            part='snippet',\n",
        "            id=self.channel_id\n",
        "        )\n",
        "        channel_response = channel_response.execute()\n",
        "        \n",
        "        return(channel_response)\n",
        "    \n",
        "    def get_videos_list(self):\n",
        "        # Receive @starwars channel info.\n",
        "        channel_response = self.get_channel_info()\n",
        "        \n",
        "        # Extract the channel creation date\n",
        "        channel_created = channel_response['items'][0]['snippet']['publishedAt']\n",
        "        channel_created_date = parser.parse(channel_created)\n",
        "        \n",
        "        # Define the search query parameters\n",
        "        # Find videos that contain 'Trailer'.\n",
        "        query = 'Trailer'\n",
        "        # Number of videos to retrieve per code execution.\n",
        "        num_results = 5 \n",
        "        # All videos published after @starwars' YouTube channel conception.\n",
        "        published_after = channel_created_date.strftime('%Y-%m-%dT%H:%M:%SZ')\n",
        "        \n",
        "        # Call the YouTube Data API to retrieve the list of videos.\n",
        "        search_response = self.youtube.search().list(\n",
        "            part='id,snippet',\n",
        "            channelId=self.channel_id,\n",
        "            q=query,\n",
        "            type='video',\n",
        "            maxResults=num_results,\n",
        "            publishedAfter=published_after\n",
        "        )\n",
        "        search_response = search_response.execute()\n",
        "        \n",
        "        return(search_response)\n",
        "    \n",
        "    def get_comments(self):\n",
        "        videos_info_list = self.get_videos_list()\n",
        "        \n",
        "        # Extract the video ids and titles from the API response\n",
        "        video_ids = []\n",
        "        video_titles = []\n",
        "        for info in videos_info_list.get('items', []):\n",
        "            video_ids.append(info['id']['videoId'])\n",
        "            video_titles.append(info['snippet']['title'])\n",
        "            \n",
        "        # Call the YouTube Data API to retrieve comments for the videos.\n",
        "        comments = []\n",
        "        for video_id, video_title in zip(video_ids, video_titles):\n",
        "            results = self.youtube.commentThreads().list(\n",
        "                part='snippet',\n",
        "                videoId=video_id,\n",
        "                textFormat='plainText',\n",
        "                maxResults=10\n",
        "            )\n",
        "            results = results.execute()\n",
        "            \n",
        "            while results:\n",
        "                for item in results['items']:\n",
        "                    comment = item['snippet']['topLevelComment']['snippet']['textDisplay']\n",
        "                    author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']\n",
        "                    date = item['snippet']['topLevelComment']['snippet']['publishedAt']\n",
        "                    comments.append([video_title, author, comment, date])\n",
        "\n",
        "                    # Stop after 1000 comments have been collected\n",
        "                    if len(comments) == 50:\n",
        "                        break\n",
        "\n",
        "                if len(comments) == 50:\n",
        "                    break\n",
        "                \n",
        "        # Convert the list of comments to a pandas DataFrame\n",
        "        df_comments = pd.DataFrame(comments, columns=['video_title', 'author', 'comment', 'date'])\n",
        "        \n",
        "        return(df_comments)"
      ],
      "metadata": {
        "id": "mZodN5nPM3Co"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}