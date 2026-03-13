#!/usr/bin/env python
# coding: utf-8

# ============================================================
# Import Libraries
# ============================================================

import requests
import numpy as np
import pandas as pd
import re
import json
from bs4 import BeautifulSoup
from selenium import webdriver
import time
from tqdm import tqdm
import pickle
import random
from nameparser import HumanName
from fuzzywuzzy import process

# pip install nameparser
# pip install fuzzywuzzy


# ============================================================
# Root Links of Interest
# ============================================================

ratemd_link = "https://www.ratemds.com"
healthgrades_link = "https://www.healthgrades.com/"
webmd_link = "https://doctor.webmd.com/providers/specialty/pulmonology/alabama/alabaster"


# ============================================================
# States and Specialties of Interest
# ============================================================

coded_states = [
    'alabama', 'alaska', 'arizona', 'arkansas', 'california',
    'colorado', 'connecticut', 'delaware', 'florida', 'georgia',
    'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas',
    'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts',
    'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
    'nebraska', 'nevada', 'new-hampshire', 'new-jersey', 'new-mexico',
    'new-york', 'north-carolina', 'north-dakota', 'ohio', 'oklahoma',
    'oregon', 'pennsylvania', 'rhode-island', 'south-carolina',
    'south-dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west-virginia', 'wisconsin', 'wyoming'
]

# Note: missing Mycobacterial (covered by pulmonologists),
# interstitial lung disease (covered by pulmonologists?),
# and Bronchiectasis (covered by immunology, thoracic surgery, and other cats?)
coded_specialties = [
    'urology',
    'pulmonology',
    'infectious-disease',
    'cardiovascular-disease',
    'orthodontics',
    'oral-maxillofacial-surgery',
    'immunology',
    'thoracic-surgery'
]


# ============================================================
# Helper Functions
# ============================================================

def code_city(city):
    """Convert a city name into a URL-safe coded city name."""
    coded_name = city.lower().replace(" ", "-")
    return coded_name


def get_cities(state_link):
    """Return all coded city names listed on a state's specialty page."""
    response = requests.get(state_link)
    soup = BeautifulSoup(response.content, 'html.parser')
    state_tags = soup.find_all('a', class_='centerwell-list-item')
    coded_cities = [code_city(x.get_text(strip=True)) for x in state_tags]
    return coded_cities


def scrape_page(link, macro_parameter, state_parameter, city_parameter):
    """Scrape a single results page and return a DataFrame of doctor records."""
    response = requests.get(link)
    soup = BeautifulSoup(response.content, 'html.parser')

    names = []
    detail_specialty = []
    links = []
    addresses = []
    macro_specialty = []
    states = []
    cities = []

    card_content_divs = soup.find_all('div', class_='card-content')

    for card_content_div in card_content_divs:
        if card_content_div is not None:
            doc_name = card_content_div.find('h2').find('a').get_text(strip=True)
            names.append(doc_name)
            doc_specialty = card_content_div.find('p', class_='prov-specialty').get_text(strip=True)
            detail_specialty.append(doc_specialty)
            doc_link = card_content_div.find('h2').find('a').get('href')
            links.append(doc_link)
            doc_address = card_content_div.find('address').find('span').get_text(strip=True)
            addresses.append(doc_address)
            states.append(state_parameter)
            cities.append(city_parameter)
            macro_specialty.append(macro_parameter)
        else:
            print("No data found for a card.")

    page_data = {
        "state": states,
        "city": cities,
        "macro_specialty": macro_specialty,
        "names": names,
        "detail_specialty": detail_specialty,
        "links": links,
        "addresses": addresses
    }
    df = pd.DataFrame(page_data)
    return df


def scrape_pages(base_link, macro_parameter, state_parameter, city_parameter):
    """Scrape all paginated results for a given city/specialty and return a combined DataFrame."""
    page_number = 1
    new_adds = True
    base_df = pd.DataFrame(columns=["state", "city", "macro_specialty", "names", "detail_specialty", "links", "addresses"])

    # Scrape first page
    sample_link = base_link + "?pagenumber=" + str(page_number)
    new_df = scrape_page(sample_link, macro_parameter, state_parameter, city_parameter)
    combined_df = pd.concat([base_df, new_df], axis=0)

    # Continue scraping until no new names are found
    while new_adds:
        page_number += 1
        sample_link = base_link + "/" + "?pagenumber=" + str(page_number)
        new_df = scrape_page(sample_link, macro_parameter, state_parameter, city_parameter)
        print(new_df.names.iloc[0])
        if new_df.iloc[0].names == combined_df.iloc[0].names:
            print("No new names found, end of pages reached")
            new_adds = False
        else:
            combined_df = pd.concat([combined_df, new_df])
            print("Page " + str(page_number) + " scraped successfully.")

    return combined_df


# ============================================================
# Main Scraping Loop — All Specialties
# ============================================================

master_df = pd.DataFrame(columns=["state", "city", "macro_specialty", "names", "detail_specialty", "links", "addresses"])

base_link = "https://doctor.webmd.com/providers/specialty/"
for specialty in tqdm(coded_specialties, desc="Specialties"):
    print("Checking specialty: " + specialty + "...")
    specialty_link = base_link + "/" + specialty
    macro_parameter = specialty
    for state in tqdm(coded_states, desc="States", leave=False):
        print("Checking state: " + state + "...")
        state_link = specialty_link + "/" + state
        state_parameter = state
        coded_cities = get_cities(state_link)
        for city in tqdm(coded_cities, desc="Cities", leave=False):
            time.sleep(2)
            city_parameter = city
            city_link = state_link + "/" + city
            print(city)
            print(city_link)
            try:
                new_city_df = scrape_pages(city_link, macro_parameter, state_parameter, city_parameter)
                master_df = pd.concat([master_df, new_city_df])
            except:
                pass


# ============================================================
# Continuation Loop — Resume Urology from Missouri
# ============================================================
# Use when the main loop is interrupted partway through.

target_specialty = "urology"
target_state = "missouri"
index_of_state = coded_states.index(target_state)
new_coded_states = coded_states[24:]

temp_master = pd.DataFrame(columns=["state", "city", "macro_specialty", "names", "detail_specialty", "links", "addresses"])

base_link = "https://doctor.webmd.com/providers/specialty/"
specialty_link = base_link + "/" + target_specialty
macro_parameter = target_specialty
for state in tqdm(new_coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link = specialty_link + "/" + state
    state_parameter = state
    coded_cities = get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(2)
        city_parameter = city
        city_link = state_link + "/" + city
        print(city)
        print(city_link)
        try:
            new_city_df = scrape_pages(city_link, macro_parameter, state_parameter, city_parameter)
            temp_master = pd.concat([temp_master, new_city_df])
        except:
            pass

# Combine urology datasets and save
urology_df = pd.concat([master_df, temp_master])
urology_df.to_pickle("./urology_all.pkl")


# ============================================================
# Pulmonology Scraping Loop
# ============================================================

temp_master = pd.DataFrame(columns=["state", "city", "macro_specialty", "names", "detail_specialty", "links", "addresses"])

target_specialty = coded_specialties[1]
base_link = "https://doctor.webmd.com/providers/specialty/"
specialty_link = base_link + "/" + target_specialty
macro_parameter = target_specialty
for state in tqdm(coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link = specialty_link + "/" + state
    state_parameter = state
    coded_cities = get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(2)
        city_parameter = city
        city_link = state_link + "/" + city
        print(city)
        print(city_link)
        try:
            new_city_df = scrape_pages(city_link, macro_parameter, state_parameter, city_parameter)
            temp_master = pd.concat([temp_master, new_city_df])
        except:
            pass

temp_master.to_pickle("./pulmonology_all.pkl")


# ============================================================
# Special Infections Scraping Loop
# ============================================================

temp_master = pd.DataFrame(columns=["state", "city", "macro_specialty", "names", "detail_specialty", "links", "addresses"])

target_specialty = coded_specialties[2]
base_link = "https://doctor.webmd.com/providers/specialty/"
specialty_link = base_link + "/" + target_specialty
macro_parameter = target_specialty
for state in tqdm(coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link = specialty_link + "/" + state
    state_parameter = state
    coded_cities = get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(1)
        city_parameter = city
        city_link = state_link + "/" + city
        print(city)
        print(city_link)
        try:
            new_city_df = scrape_pages(city_link, macro_parameter, state_parameter, city_parameter)
            temp_master = pd.concat([temp_master, new_city_df])
        except:
            pass

temp_master.to_pickle("./special_all.pkl")


# ============================================================
# Clean the Dataset
# ============================================================

special_df = pd.read_pickle("./special_all.pkl")
urology_df = pd.read_pickle("./urology_all.pkl")
pulmonology_df = pd.read_pickle("./pulmonology_all.pkl")

# Remove duplicate rows
special_df = special_df[special_df.duplicated() == False]
urology_df = urology_df[urology_df.duplicated() == False]
pulmonology_df = pulmonology_df[pulmonology_df.duplicated() == False]

# Combine into master DataFrame
master_df = pd.concat([urology_df, pulmonology_df, special_df], axis=0).reset_index(drop=True)

# Select relevant columns
tgt_cols = ["names", "macro_specialty", "state", "links", "addresses"]
clean_df = master_df.loc[:, tgt_cols]

# Group by name/specialty/state and aggregate addresses and links into lists
clean_df = clean_df.groupby(["names", "macro_specialty", "state"]).agg({"links": list, "addresses": list})
clean_df = clean_df.reset_index()
# Result: ~93,869 records


# ============================================================
# Email Matching via Fuzzy Name Matching
# ============================================================

# Load and clean emails CSV
emails = pd.read_csv("email_2.csv")
emails = emails.reset_index(drop=True).loc[:, ["name", "email"]]
# Note: ~1394 names without emails

# Parse names from email list
emails_has_name = emails[emails.name.isnull() == False]
firstname, middlename, lastname, processed_name = [], [], [], []
for name in emails_has_name.name:
    human = HumanName(name)
    firstname.append(human.first)
    middlename.append(human.middle)
    lastname.append(human.last)
    temp = (human.first + " " + human.last).lower().replace(".", "")
    processed_name.append(temp)

emails_has_name["first_name"] = firstname
emails_has_name["middle_name"] = middlename
emails_has_name["last_name"] = lastname
emails_has_name["processed_name"] = processed_name

# Parse names from doctors list
firstname, middlename, lastname, processed_name = [], [], [], []
for name in clean_df.names:
    human = HumanName(name)
    firstname.append(human.first)
    middlename.append(human.middle)
    lastname.append(human.last)
    temp = (human.first + " " + human.last).lower().replace(".", "")
    processed_name.append(temp)

clean_df["first_name"] = firstname
clean_df["middle_name"] = middlename
clean_df["last_name"] = lastname
clean_df["processed_name"] = processed_name

# Initialize match columns
clean_df["matched_name"] = None
clean_df["matched_score"] = None

# Fuzzy match doctors to email names
threshold = 95  # Adjust based on desired strictness
matches = {}
processed_list1 = list(emails_has_name.processed_name)  # Names with emails assigned
processed_list2 = list(clean_df.processed_name)          # Doctors list

for name2 in tqdm(processed_list2):
    match, score = process.extractOne(name2, processed_list1)
    if score >= threshold:
        matches[name2] = {'match': match, 'score': score}

# Print matched pairs and scores
for name2, match_info in matches.items():
    print(f"Name2: {name2}, Match: {match_info['match']}, Score: {match_info['score']}")
