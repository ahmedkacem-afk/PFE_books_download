# Project Name

## Description

This project is a Python web scraper that extracts company names and associated Google Drive links from [recruter.tn](https://www.recruter.tn), downloads the linked PDFs, and saves them locally. In the main.py .

## Setup Instructions
make sure you change the directory  after cloning to PFE_books_download/sitemap_scraping

### Option 1: Running with Docker

If you want to run the project inside a Docker container, follow these steps:

1. **Build the Docker Image**:
   First, build the Docker image by running:
   ```bash
   docker build -t image_name .
   ```
2. **run the Docker Image**:
   Then, run the Docker image by running:
   ```bash
   docker run --rm image_name
   ```

### Option 2: Running without Docker

If you have python installed and you want to run the project without Docker, follow these steps:

1. **Install the Required Packages**:
   First, install the required packages by running:
   ```bash
   pip install -r requirements.txt
   ```
2. **Run the Project**:
   Then, run the project by running:
   ```bash
    python scriptpfe.py
   ```
