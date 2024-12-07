import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union
import threading
import os
# Constants
PFE_REAL = ["2025", "pfe-book"]
HEADERS = {"User-Agent": "Mozilla/5.0"}
FILTER_DATE = "2024-11-01"  # Set your cutoff date here
DOWNLOAD_PATH="pfe_pdf"

downloads={"successful_downloads":[],"failed_downloads":[],"already_downloaded":[]}
def check_pfe_link(link: str) -> bool:
    """
    Checks if a link contains any of the keywords in PFE_REAL.
    """
    try:
        # Check each keyword
        match = any(keyword.lower() in link.lower() for keyword in PFE_REAL)
        if match:
            print(f"Checking link: {link} -> Match: {match}")  # Debug log
        return match
    except Exception as e:
        print(f"Error checking link: {link}, Error: {e}")
        return False


def filter_links_by_keywords(df: pd.DataFrame, link_column: str = "loc") -> pd.DataFrame:
    """
    Filters DataFrame rows where links in the specified column match PFE_REAL.
    """
    try:
        if link_column not in df.columns:
            print(f"Column '{link_column}' not found in DataFrame.")
            return pd.DataFrame()

        # Apply the filtering logic
        print(f"Before filtering, DataFrame size: {df.shape}")  # Debug log
        filtered_df = df[df[link_column].apply(check_pfe_link)].reset_index(drop=True)
        print(f"After filtering, DataFrame size: {filtered_df.shape}")  # Debug log
        return filtered_df
    except Exception as e:
        print(f"Error filtering DataFrame: {e}")
        return pd.DataFrame()

def fetch_and_parse_xml(url: str) -> Union[pd.DataFrame, None]:
    """
    Fetches an XML URL and parses it into a pandas DataFrame.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        df = pd.read_xml(response.content)
        return df
    except Exception as e:
        print(f"Failed to fetch or parse {url}: {e}")
        return None


def filter_dataframe_by_date(df: pd.DataFrame, date_column: str = "lastmod", cutoff_date: str = FILTER_DATE) -> pd.DataFrame:
    """
    Filters rows in the DataFrame based on a cutoff date.
    """
    if date_column in df.columns:
        return df[df[date_column] >= cutoff_date].reset_index(drop=True)
    return df



def process_sitemap(url: str) -> pd.DataFrame:
    """
    Processes a sitemap URL, filters it by date, and returns the resulting DataFrame.
    """
    df = fetch_and_parse_xml(url)
    if df is not None:
        df = filter_dataframe_by_date(df)
    return df


def process_nested_sitemaps(urls: List[str]) -> List[pd.DataFrame]:
    """
    Processes multiple sitemap URLs in parallel and returns a list of DataFrames.
    """
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_sitemap, url): url for url in urls}
        for future in futures:
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")
    return results


def get_sitemap_urls(robots_url: str) -> List[str]:
    """
    Extracts sitemap URLs from a robots.txt URL.
    """
    try:
        response = requests.get(robots_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        text = soup.get_text().split("\n")
        sitemaps = [line.split(": ")[1] for line in text if "Sitemap" in line]
        return sitemaps
    except Exception as e:
        print(f"Failed to fetch or parse robots.txt {robots_url}: {e}")
        return []


def get_remaining_sitemaps(df: pd.DataFrame) -> List[str]:
    """
    Extracts remaining sitemap URLs from a DataFrame.
    """
    if "loc" in df.columns:
        remaining = df[df["loc"].str.endswith(".xml")]["loc"].tolist()
        df.drop(df[df["loc"].str.endswith(".xml")].index, inplace=True)
        return remaining
    return []


def main_sitemap_processing(sitemap_urls: List[str]) -> pd.DataFrame:
    """
    Processes multiple sitemaps, consolidates them, and filters by PFE_REAL keywords.
    """
    all_dataframes = process_nested_sitemaps(sitemap_urls)
    combined_df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
    
    return combined_df

def extract_company_name(link) -> str:
    company_name=link.split("/")[-2]
    
    return company_name
def get_file_id( link:str ,companies:list) -> str:
    print(f"Processing link: {link}")
    try:
        if not link:
            return None
        res = requests.get(link, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")
        company=extract_company_name(link)
        print(f"Company: {company}")
        with counter_lock:
            companies.append(company)
        iframe = soup.find("iframe",src=lambda x: x and x.startswith("https://drive.google.com"))
        if iframe:
            src = iframe["src"].split("/")
            id_index = src.index('d') + 1
            return src[id_index]
    except Exception as e:
        print(f"Error processing {company}: {e}")
    return None   

counter_lock = threading.Lock()
def download_file(file_id, company):
    file_path=f'./{DOWNLOAD_PATH}/{company}.pdf'
    try:
        if not file_id:
            return
        download_url = f'https://drive.google.com/uc?export=download&id={file_id}'
        response = requests.get(download_url)
        if response.status_code == 200 :
            if not os.path.exists(file_path):
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f"{company}: File downloaded successfully!")
                with counter_lock:
                    downloads["successful_downloads"].append(company)
                
            else:
                print(f"{company}.pdf: file already exists. ")
                with counter_lock:
                    downloads["already_downloaded"].append(company)
                

                
        else:
            print(f"{company}: Failed to download the file. ")
            with counter_lock:
                downloads["failed_downloads"].append(company)
    

    except Exception as e:
        print(f"Error downloading {company}: {e}")
def get_existing_file_name():
    file_names= os.listdir(DOWNLOAD_PATH)
    file_names=[file_name.split(".")[0] for file_name in file_names]
    return file_names
# Main Execution
if __name__ == "__main__":
    robots_url = "https://www.recruter.tn/robots.txt"
    sitemap_urls = get_sitemap_urls(robots_url)
    final_df = pd.DataFrame()
    try:
        os.mkdir(DOWNLOAD_PATH)
        print(f"Directory '{DOWNLOAD_PATH}' created successfully.")
    except FileExistsError:
        print(f"Directory '{DOWNLOAD_PATH}' already exists.")
    except PermissionError:
        print(f"Permission denied: Unable to create '{DOWNLOAD_PATH}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

    while sitemap_urls:
        print(f"Processing {len(sitemap_urls)} sitemap(s)...")
        # Process the current set of sitemap URLs
        current_df = main_sitemap_processing(sitemap_urls)
        # Concatenate the current results to the final DataFrame
        final_df = pd.concat([final_df, current_df], ignore_index=True)
        # Get remaining sitemaps from the current DataFrame
        sitemap_urls = get_remaining_sitemaps(final_df)
    download_df=pd.DataFrame()
    final_df = filter_links_by_keywords(final_df)
    final_df=final_df.drop_duplicates(subset=["loc"])
    existing_file_names=get_existing_file_name()
   
    # Filter out rows where any substring in `existing_file_names` is found in `final_df["loc"]`
    final_df = final_df[~final_df["loc"].apply(lambda loc: any(substring in loc for substring in existing_file_names))]
    final_df.sort_values(by="lastmod", ascending=False, inplace=True)
    print(f"++++++++++++++++++++++++++++++++++++++++++++++++final_df\n: {final_df}")  
    print(f"**********************************************************:      {len(final_df)}")
    """ for link in final_df["loc"]:
        existing_companies.append(extract_company_name(link)) """
    counter_lock = threading.Lock()
    companies = []
    
    file_ids = []
    for index,link in enumerate(final_df["loc"]):
        file_id = get_file_id(link, companies) 
        print(f"file_id {index}: {file_id}")
        file_ids.append(file_id) # Call the function for each link and company pair
    download_df["file_id"] = file_ids # Append the result to the 'file_id' column
    download_df["company"]=companies
    downloads["already_downloaded"]=existing_file_names
    with ThreadPoolExecutor() as executor:
        executor.map(download_file, download_df["file_id"], download_df["company"])
    print(f"successful downloads :{downloads['successful_downloads']}\n+++\n Number: {len(downloads['successful_downloads'])}\n/////|||||||********\n failed downloads {downloads['failed_downloads']} \n+++\n Number: {len(downloads['failed_downloads'])}\n////////////**********\n already downloaded {downloads['already_downloaded']} \n+++ \nNumber: {len(downloads['already_downloaded'])} ")
