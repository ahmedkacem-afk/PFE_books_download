import pandas as pd
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
#change the download path to your desired path

download_path="../pfe_pdf"

url = "https://www.recruter.tn/300-pfe-book-2025/"

headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)

df = pd.read_html(url, storage_options=headers)
df = df[0]
df = pd.DataFrame(df)

def get_link_by_text(text):
    soup = BeautifulSoup(response.text, "html.parser")
    a_tag = soup.find("a", string=text)  # Find the <a> tag with matching text
    all_a = soup.find_all("a")
    if a_tag:
        return a_tag["href"]
    else:
        for link in all_a:
            has_image = link.find("img") is not None  # Check for an <img> tag
            has_text = link.find(string=text) is not None  # Check for exact matching text
            if has_image and has_text:
                return link["href"]

dd = pd.DataFrame()
all_items = df.stack().tolist()
dd["company"] = all_items
dd["link"] = dd["company"].apply(get_link_by_text)

def get_file_id(company, link):
    try:
        if not link:
            return None
        res = requests.get(link, headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        iframe = soup.find_all("iframe")
        if iframe:
            for i in iframe:
                if i["src"].startswith("https://drive.google.com"):
                    src = i["src"].split("/")
                    id_index = src.index('d') + 1
                    return src[id_index]
    except Exception as e:
        print(f"Error processing {company}: {e}")
    return None
def download_file(file_id, company):
    try:
        if not file_id:
            return
        download_url = f'https://drive.google.com/uc?export=download&id={file_id}'
        response = requests.get(download_url)
        if response.status_code == 200:
            with open(f'{download_path}{company}.pdf', 'wb') as f:
                f.write(response.content)
            print(f"{company}: File downloaded successfully!")
        else:
            print(f"{company}: Failed to download the file.")
    except Exception as e:
        print(f"Error downloading {company}: {e}")

# Process file IDs concurrently
with ThreadPoolExecutor() as executor:
    dd["file_id"] = list(executor.map(get_file_id, dd["company"], dd["link"]))

# Download files concurrently
with ThreadPoolExecutor() as executor:
    executor.map(download_file, dd["file_id"], dd["company"])
