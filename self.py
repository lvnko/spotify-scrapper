import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from flask import Flask, request, redirect, jsonify

# Environment variables setup
environment = os.environ.get("ENVIRONMENT")
envSuffix = f".{environment}" if environment is not None else ''
dotenv_path = f".env{envSuffix}.local"
print(f"Loading environment variables from: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path, override=True)

# Set up Flask app
app = Flask(__name__)

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-private user-read-email user-follow-read"  # Scopes for user profile data
))

# Main function to get user data
def get_user():
    cur_user = sp.me()
    print(cur_user)
    return cur_user

# Route to handle redirect
@app.route("/callback")
def callback():
    code = request.args.get("code")
    sp.auth_manager.get_access_token(code)
    return "Authentication successful! You can close this window."

@app.route("/user/follower")
def list_follower():
    cur_user = get_user()
    followers_count = cur_user["followers"]["total"]
    return jsonify({
        "user_followers": cur_user["followers"]
    })

@app.route("/recomm/genre")
def recomm_genres():
    results = sp.recommendation_genre_seeds()
    return jsonify({
        "genres": results
    })

if __name__ == "__main__":
    # get_user()
    app.run(port=8888)
