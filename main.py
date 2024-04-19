from datetime import datetime, timedelta
import bson.binary
from pymongo import MongoClient

import pandas as pd
import requests
import streamlit as st
from apify_client import ApifyClient
import bson

api_token = "apify_api_hX3TvvuRcz9UCjbW7aWg4nwTwCdxjl0wIS74"

# Initialize the ApifyClient with your Apify API token
apify_client = ApifyClient(api_token)

mongo_client = MongoClient('mongodb+srv://enesceman1:k89b71Hjw0j0GyAx@cluster0.3zssmv0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')  # Adjust as needed
db = mongo_client['test']

def is_run_needed(last_run_str):
    last_run_date = datetime.strptime(last_run_str, '%Y-%m-%d')
    if datetime.now() - last_run_date < timedelta(days=7):
        return False
    return True


def run_actors_for_posts_and_profile(username, collection):
    # Prepare the Actor input
    run_input_profile = {"usernames": [username], "resultsLimit": 1,}
    run_input_posts = {"username": [username], "resultsLimit": 5,}
   
    # Run the Actor and wait for it to finish
    run = apify_client.actor("apify/instagram-profile-scraper").call(run_input=run_input_profile)
    run_post_scraper = apify_client.actor("apify/instagram-post-scraper").call(run_input=run_input_posts)

    # API endpoints for the datasets created in the last runs
    last_posts_scraped_api_endpoint = f"https://api.apify.com/v2/acts/apify~instagram-post-scraper/runs/last/dataset/items?token={api_token}"   
    last_profile_scraped_api_endpoint = f"https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs/last/dataset/items?token={api_token}"

    # Send a GET request to the API endpoint and parse the JSON response
    response_posts = requests.get(last_posts_scraped_api_endpoint).json()
    response_profile = requests.get(last_profile_scraped_api_endpoint).json()
    profile_pic_url = response_profile[0]['profilePicUrlHD']
    
    latest_posts = [{
        'displayUrl': post['displayUrl'], 
        'commentsCount' : post['commentsCount'], 
        'likesCount': post['likesCount']
        } for post in response_profile[0]['latestPosts'][:5]]
    
    images_binaries = []
    
    for post in latest_posts:
        response = requests.get(post['displayUrl'])
        if response.status_code == 200:
            binary_data = bson.binary.Binary(response.content)
            images_binaries.append({"binary_img": binary_data, "commentsCount" : post['commentsCount'], 'likesCount': post['likesCount']})

    response_profile_image = requests.get(profile_pic_url)
    if response_profile_image.status_code == 200:
        profile_image_data = bson.binary.Binary(response_profile_image.content)
   
    user_data = {
        'profile': response_profile[0],
        'posts': response_posts,
        'last_run': datetime.now().strftime('%Y-%m-%d'),
        'profile_pic': profile_image_data,
        'five_images': images_binaries
    }
    
    result = collection.replace_one({}, user_data, upsert=True)
    return result

# Define a function to fetch data from Apify
def fetch_data(username):
    collection = db[username]
    
    document = collection.find_one()
    
    if document is None or is_run_needed(document['last_run']):
        run_actors_for_posts_and_profile(username, collection)
        document = collection.find_one()
    
    js_posts = document['posts']
    js_profile = document['profile']
    profile_pic = document['profile_pic']
    five_images = document['five_images']
    
    
    df_posts = pd.DataFrame(js_posts)
    
    hashtags = df_posts[['hashtags', 'likesCount']].explode('hashtags').groupby('hashtags').agg({'likesCount':['count','mean']})
    hashtags.columns = ['count', 'average_likes']
    most_liked_hashtags = hashtags.query('count>0').sort_values(by='average_likes', ascending=False)
    
    sorted_last_50_posts = df_posts[['likesCount', 'timestamp', 'displayUrl', 'commentsCount']].sort_values(by='timestamp', ascending=False)
    
    relevant_data = {}
    
    # response_profile_pic = requests.get(profile_pic_url)
    # profile_pic_filename = os.path.join(static_path, f"{username}_profile_pic.png")
    
    # with open(profile_pic_filename, "wb") as f:
    #     f.write(response_profile_pic.content)
    
    relevant_data['full_name'] = js_profile['fullName']
    relevant_data['biography'] = js_profile['biography']
    relevant_data['posts_count'] = js_profile['postsCount']
    relevant_data['total_followers_count'] = js_profile['followersCount']
    relevant_data['isBusiness'] = js_profile['isBusinessAccount']

    relevant_data['likes_count'] = df_posts['likesCount'].sum()
    relevant_data['likes_avg'] = df_posts['likesCount'].mean()
    relevant_data['comments_count'] = df_posts['commentsCount'].sum()
    relevant_data['engagement_rate'] = (relevant_data['likes_count'] + relevant_data['comments_count'] / len(df_posts)) / relevant_data['total_followers_count'] * 100

    return {"relevant_data":relevant_data, "sorted_last_10_posts":sorted_last_50_posts.head(10), "most_liked_hashtags": most_liked_hashtags, "profile_pic": profile_pic, "five_images": five_images}

# Streamlit app
def main():
    st.title("Instagram Activity - Last 10 Days")

    # Input username using a textbox
    username = st.text_input("Please enter the username:")

    if st.button("Fetch Data"):
        if username:
            fetched_data = fetch_data(username)
            relevant_data = fetched_data['relevant_data']
            sorted_last_10_posts  = fetched_data['sorted_last_10_posts']
            most_liked_hashtags  = fetched_data['most_liked_hashtags'].head(10)
            profile_pic = fetched_data['profile_pic']
            five_images = fetched_data['five_images']
            # Display the fetched data
            if relevant_data:
                    st.write("Full Name:", relevant_data['full_name'])
                    col1, col2 = st.columns(2)
                    col1.image(profile_pic)
                    col2.write(f"Biography: {relevant_data['biography']}")
                    col2.write(f"Is Business Account: {relevant_data['isBusiness']}")
                    col2.write(f"Total Number Of Followers: {relevant_data['total_followers_count']}")
                    col2.write(f"Total Number Of Posts: {relevant_data['posts_count']}")
                    col2.write(f"Average Likes per Post (Last 10 Days): {relevant_data['likes_avg']}")
                    col2.write(f"Engagement Rate (Last 10 days): {relevant_data['engagement_rate']}")
                    
                    col3, col4 = st.columns(2)
                    col3.subheader('Most Frequent Hashtags')
                    col3.table(most_liked_hashtags.sort_values('count',ascending=False)[['count']])
                    
                    col4.subheader("Most Liked Hashtags")
                    col4.table(most_liked_hashtags.sort_values('average_likes',ascending=False)[['average_likes']])
                    
                    st.subheader("Likes Count Over The Last 5 Posts")
                    st.bar_chart(sorted_last_10_posts.set_index("timestamp")["likesCount"])
                    
                    st.subheader("Last 5 Posts")
                    
                    col5, col6, col7, col8, col9 = st.columns(5)
                    
                    colNum=5
                    # for index, row in five_images.iterrows():
                    for image in reversed(five_images):
                        col = locals()[f"col{colNum}"]  # Get the column object dynamically using locals()
                        col.image(image['binary_img'])
                        col.write(f"Likes: {image['likesCount']}")
                        col.write(f"Comments: {image['commentsCount']}")
                        colNum += 1
                    
                    
            else:
                st.error("No data found for the entered username.")
        else:
            st.error("Please enter a username.")


if __name__ == "__main__":
    main()