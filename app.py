import os
import json
import time
import httpx
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_FOLDER = "data"
ERROR_FOLDER = "error"
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(ERROR_FOLDER, exist_ok=True)

INSTAGRAM_DOC_ID = "8845758582119845"

def get_display_url(post_url_or_shortcode):
    # Extract shortcode from URL or use directly
    if "instagram.com" in post_url_or_shortcode:
        shortcode = post_url_or_shortcode.split("/p/")[-1].split("/")[0]
    else:
        shortcode = post_url_or_shortcode.strip()

    variables = {
        "shortcode": shortcode,
        "fetch_comment_count": 0,
        "parent_comment_count": 0,
        "has_threaded_comments": True,
        "hoisted_comment_id": None,
        "hoisted_reply_id": None,
        "hoisted_reply_author_id": None
    }

    url = f"https://www.instagram.com/graphql/query/?doc_id={INSTAGRAM_DOC_ID}&variables={quote(json.dumps(variables))}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-IG-App-ID": "936619743392459",
        "Accept": "*/*"
    }

    try:
        response = httpx.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["data"]["xdt_shortcode_media"]["display_url"]
    except Exception as e:
        print(f"Error fetching thumbnail for {shortcode}: {e}")
        return None

def process_single_post(post):
    shortcode = post.get("shortcode")
    handle = post.get("handle", "unknown")

    if not shortcode:
        return f"⚠️ Skipping post for handle {handle} (no shortcode)"

    output_path = os.path.join(DATA_FOLDER, f"{shortcode}.json")
    if os.path.exists(output_path):
        return f"⏭️  {handle} - Already processed"

    url = f"https://www.instagram.com/p/{shortcode}/"
    try:
        display_url = get_display_url(url)
        if display_url:
            post["display_url"] = display_url
            with open(output_path, "w") as out_file:
                json.dump(post, out_file, indent=2)
            return f"✅ {handle} saved to {output_path}"
        else:
            error_path = os.path.join(ERROR_FOLDER, f"{shortcode}.json")
            with open(error_path, "w") as out_file:
                json.dump(post, out_file, indent=2)
            return f"❌ {handle} - Thumbnail not found, saved to {error_path}"
    except Exception as e:
        error_path = os.path.join(ERROR_FOLDER, f"{shortcode}.json")
        with open(error_path, "w") as out_file:
            json.dump(post, out_file, indent=2)
        return f"❌ {handle} - Error: {e}, saved to {error_path}"

def get_last_processed_shortcode():
    json_files = [
        os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if f.endswith(".json")
    ]
    if not json_files:
        return None
    latest_file = max(json_files, key=os.path.getctime)
    return os.path.splitext(os.path.basename(latest_file))[0]

def process_posts(json_path):
    with open(json_path, "r") as f:
        posts = json.load(f)

    last_shortcode = get_last_processed_shortcode()
    print(f"🕵️ Last processed shortcode: {last_shortcode}")

    if last_shortcode:
        for i, post in enumerate(posts):
            if post.get("shortcode") == last_shortcode:
                posts = posts[i+1:]
                break

    if not posts:
        print("✅ All posts already processed.")
        return

    print(f"🚀 Processing {len(posts)} posts")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(process_single_post, post) for post in posts]
        for future in as_completed(futures):
            print(future.result())

def retry_single_post(error_file):
    path = os.path.join(ERROR_FOLDER, error_file)
    with open(path, "r") as f:
        post = json.load(f)

    shortcode = post.get("shortcode")
    handle = post.get("handle", "unknown")
    url = f"https://www.instagram.com/p/{shortcode}/"

    try:
        display_url = get_display_url(url)
        if display_url:
            post["display_url"] = display_url
            output_path = os.path.join(DATA_FOLDER, f"{shortcode}.json")
            with open(output_path, "w") as out_file:
                json.dump(post, out_file, indent=2)
            os.remove(path)
            return f"🔄 ✅ {handle} - Successfully retried and saved to {output_path}"
        else:
            return f"🔄 ❌ {handle} - Still no thumbnail"
    except Exception as e:
        return f"🔄 ❌ {handle} - Error on retry: {e}"

def retry_error_posts():
    error_files = [f for f in os.listdir(ERROR_FOLDER) if f.endswith(".json")]
    if not error_files:
        print("✅ No error posts to retry.")
        return

    print(f"🔁 Retrying {len(error_files)} error posts")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(retry_single_post, file) for file in error_files]
        for future in as_completed(futures):
            print(future.result())

# 🔧 Run the script
if __name__ == "__main__":
    process_posts("test.instagramPosts.json")  # Uncomment to run full processing
    # retry_error_posts()  # Run only retry logic