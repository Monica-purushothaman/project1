from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
import markdown2
from PIL import Image
import sqlite3
import duckdb
import csv
import json
import requests

# Initialize FastAPI app
app = FastAPI()

# Ensure all operations are restricted to the /data directory
DATA_DIR = "/data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Security checks
def validate_path(file_path: str):
    if not file_path.startswith(DATA_DIR):
        raise HTTPException(status_code=403, detail="Access to this path is forbidden.")

# Task B9: Convert Markdown to HTML
@app.post("/convert-markdown")
async def convert_markdown(file_path: str = Query(..., description="Path to the Markdown file")):
    validate_path(file_path)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    with open(file_path, "r") as md_file:
        markdown_content = md_file.read()
    
    html_content = markdown2.markdown(markdown_content)
    html_file_path = file_path.replace(".md", ".html")
    
    with open(html_file_path, "w") as html_file:
        html_file.write(html_content)
    
    return {"message": "Markdown converted to HTML", "html_file": html_file_path}

# Task B3: Fetch data from an API and save it
@app.post("/fetch-api-data")
async def fetch_api_data(api_url: str, save_path: str):
    validate_path(save_path)
    response = requests.get(api_url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch data from API.")
    
    with open(save_path, "w") as file:
        file.write(response.text)
    
    return {"message": "Data fetched and saved successfully", "save_path": save_path}

# Task B7: Compress or resize an image
@app.post("/resize-image")
async def resize_image(file_path: str, width: int, height: int):
    validate_path(file_path)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    with Image.open(file_path) as img:
        img_resized = img.resize((width, height))
        resized_path = file_path.replace(".", "_resized.")
        img_resized.save(resized_path)
    
    return {"message": "Image resized successfully", "resized_path": resized_path}

# Task B10: Filter a CSV file and return JSON data
@app.post("/filter-csv")
async def filter_csv(file_path: str, column: str, value: str):
    validate_path(file_path)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    filtered_data = []
    with open(file_path, "r") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row.get(column) == value:
                filtered_data.append(row)
    
    return JSONResponse(content=filtered_data)
