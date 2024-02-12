#!/usr/bin/env python
# coding: utf-8

# In[102]:


## import necessary libraries
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


# In[101]:


# !pip install nameparser
# !pip install fuzzywuzzy


# ## Clean the dataset

# In[3]:


special_df=pd.read_pickle("./special_all.pkl")


# In[5]:


urology_df=pd.read_pickle("./urology_all.pkl")


# In[7]:


pulmonology_df=pd.read_pickle("./pulmonology_all.pkl")


# In[12]:


## remove duplicate rows

special_df=special_df[special_df.duplicated()==False]
urology_df=urology_df[urology_df.duplicated()==False]
pulmonology_df=pulmonology_df[pulmonology_df.duplicated()==False]


# In[14]:


## create the master df and aggregate addresses

master_df=pd.concat([urology_df,pulmonology_df,special_df],axis=0).reset_index(drop=True)
master_df.head()


# In[19]:


## remove the city and detail specialty
cols=master_df.columns
cols
tgt_cols=["names","macro_specialty","state","links","addresses"]
clean_df=master_df.loc[:,tgt_cols]
clean_df.shape


# In[22]:


clean_df.head()


# In[24]:


## groupby to isolate names

clean_df=clean_df.groupby(["names","macro_specialty","state"]).agg({"links":list,"addresses":list})


# In[25]:


clean_df.shape # 93,869 docs


# In[28]:


## reset index so can loop through the names
clean_df=clean_df.reset_index()


# In[50]:


## read in the emails dad sent
emails=pd.read_csv("email_2.csv")


# In[53]:


emails=emails.reset_index(drop=True).loc[:,["name","email"]]


# In[57]:


emails.email.isnull().value_counts() # 1394 names without emails


# In[69]:


## get the name patterns via sample for emails


## lower case
# remove Dr., Mr., Mrs., Ms.,
# remove middle names
# lastname, first
random.seed(42)
sampled_names=random.sample(list(emails.name),100)
sampled_names
for x in sampled_names:
    print(x)
    name = HumanName(x)
    print(name.first)
    print(name.middle)
    print(name.last)


# In[70]:


## parse 


# In[71]:


## get the name patterns via sample for clean_df

## lowercase
# remove Dr. 
# remove , MD
# remove , DO
# remove Mr. Ms. Mrs.
# remove middle names <>.
random.seed(42)
sampled_names=random.sample(list(clean_df.names),100)
for x in sampled_names:
    print(x)
    name = HumanName(x)
    print(name.first)
    print(name.middle)
    print(name.last)


# In[110]:


## get the match parsed name for emails list
# isolate just the names that are assigned to emails
emails_has_name=emails[emails.name.isnull()==False]
firstname=[]
middlename=[]
lastname=[]
processed_name=[]
for name in emails_has_name.name:
    human=HumanName(name)
    firstname.append(human.first)
    middlename.append(human.middle)
    lastname.append(human.last)
    temp=human.first + " " + human.last # human.middle + " "
    temp=temp.lower().replace(".","")
    processed_name.append(temp)
emails_has_name["first_name"]=firstname
emails_has_name["middle_name"]=middlename
emails_has_name["last_name"]=lastname
emails_has_name["processed_name"]=processed_name


# In[111]:


## get the match parsed name for doctors list
# isolate just the names that are assigned to emails
firstname=[]
middlename=[]
lastname=[]
processed_name=[]
for name in clean_df.names:
    human=HumanName(name)
    firstname.append(human.first)
    middlename.append(human.middle)
    lastname.append(human.last)
    temp=human.first + " " + human.last # + human.middle + " "
    temp=temp.lower().replace(".","")
    processed_name.append(temp)
clean_df["first_name"]=firstname
clean_df["middle_name"]=middlename
clean_df["last_name"]=lastname
clean_df["processed_name"]=processed_name


# In[113]:


emails_has_name.head(50)


# In[114]:


## set empty match + score columns
clean_df["matched_name"]= None
clean_df["matched_score"]= None


# In[115]:


clean_df.head()


# In[127]:


## do fuzzy matching for emails that have name assigned
threshold = 95 # Adjust this threshold based on your requirements

matches = {}

# Preprocess names in list1
processed_list1 = list(emails_has_name.processed_name) ## comparison - names with emails assigned

# Preprocess names in list2
processed_list2 = list(clean_df.processed_name) ## doctors list

# Iterate through each name in list2 and find the best match in list1
for name2 in tqdm(processed_list2):
    match, score = process.extractOne(name2, processed_list1)
    if score >= threshold:
        matches[name2] = {'match': match, 'score': score}
#         clean_df.loc[i,"matched_name"]=match
#         clean_df.loc[i,"matched_score"]=score
#     else:
#         clean_df.loc[i,"matched_name"]="no match"
#         clean_df.loc[i,"matched_score"]="no match"


# In[128]:


# Print matched pairs along with match scores
for name2, match_info in matches.items():
    print(f"Name2: {name2}, Match: {match_info['match']}, Score: {match_info['score']}")


# In[129]:


emails_has_name[emails_has_name.processed_name=="brian carey"]


# In[130]:


clean_df[clean_df.processed_name=="brian arey"]


# In[2]:


## root links of interest
ratemd_link="https://www.ratemds.com"
healthgrades_link="https://www.healthgrades.com/"
webmd_link="https://doctor.webmd.com/providers/specialty/pulmonology/alabama/alabaster"


# In[122]:


print("t")


# In[2]:


## read in list of states and macro specialties of interest
coded_states=['alabama',
'alaska',
'arizona',
'arkansas',
'california',
'colorado',
'connecticut',
'delaware',
'florida',
'georgia',
'hawaii',
'idaho',
'illinois',
'indiana',
'iowa',
'kansas',
'kentucky',
'louisiana',
'maine',
'maryland',
'massachusetts',
'michigan',
'minnesota',
'mississippi',
'missouri',
'montana',
'nebraska',
'nevada',
'new-hampshire',
'new-jersey',
'new-mexico',
'new-york',
'north-carolina',
'north-dakota',
'ohio',
'oklahoma',
'oregon',
'pennsylvania',
'rhode-island',
'south-carolina',
'south-dakota',
'tennessee',
'texas',
'utah',
'vermont',
'virginia',
'washington',
'west-virginia',
'wisconsin',
'wyoming'
]

coded_specialties=['urology',
'pulmonology',
'infectious-disease',
'cardiovascular-disease',
'orthodontics',
'oral-maxillofacial-surgery',
'immunology',
'thoracic-surgery'
] 
# missing: Mycobacterial (covered by pulmonologistss, instersitial lung disease (covered by pulmonlogists?), 
# missing Bronchiectasis (covered by immunology, thoracic surgery, and other cats?)


# In[287]:


## collect cities per state on opening page (try for one state first)
base_link="https://doctor.webmd.com/providers/specialty/"+coded_specialties[0]+"/"+coded_states[0]
response=requests.get(base_link)
soup = BeautifulSoup(response.content, 'html.parser')
print(sample_link)
state_tags = soup.find_all('a', class_='centerwell-list-item')
coded_cities=[code_city(x.get_text(strip=True)) for x in state_tags]


# In[3]:


## helper function to get all cities in a state
def get_cities(state_link):
    response=requests.get(state_link)
    soup = BeautifulSoup(response.content, 'html.parser')
    state_tags = soup.find_all('a', class_='centerwell-list-item')
    coded_cities=[code_city(x.get_text(strip=True)) for x in state_tags]
    return coded_cities


# In[4]:


## helper function to convert city name into coded city name
def code_city(city):
    coded_name=city.lower().replace(" ","-")
    return coded_name


# In[7]:


## collect all pages for given specialty, state, and city
base_link=base_link+"/"+coded_cities[0]


# In[256]:


print(base_link)


# In[5]:


# master_df=pd.DataFrame(columns=["state","city","macro_specialty","names","detail_specialty","links","addresses"])


# In[8]:


## cycle through specialty, states, cities, then pages
base_link="https://doctor.webmd.com/providers/specialty/"
for specialty in tqdm(coded_specialties, desc="Specialties"):
    print("Checking specialty: " + specialty + "...")
    specialty_link=base_link+"/"+specialty
    macro_parameter=specialty
    for state in tqdm(coded_states, desc="States", leave=False):
        print("Checking state: " + state + "...")
        state_link=specialty_link+"/"+state
        state_parameter=state
        coded_cities=get_cities(state_link)
        for city in tqdm(coded_cities, desc="Cities", leave=False):
            time.sleep(2)
            city_parameter=city
            city_link=state_link+"/"+city
            print(city)
            print(city_link)
            try:
                new_city_df=scrape_pages(city_link,macro_parameter, state_parameter, city_parameter)
                master_df=pd.concat([master_df,new_city_df])
            except:
                pass


# In[28]:


## continuation loop when interrupted

target_specialty="urology"
target_state="missouri"
index_of_state = coded_states.index(target_state)
index_of_state
new_coded_states=coded_states[24:]


# In[30]:


## temp master
temp_master=pd.DataFrame(columns=["state","city","macro_specialty","names","detail_specialty","links","addresses"])


# In[32]:


## rerun code just through urology and from missouri, rerunning cities (can remove duplicates later)
base_link="https://doctor.webmd.com/providers/specialty/"
specialty_link=base_link+"/"+target_specialty
macro_parameter=target_specialty
for state in tqdm(new_coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link=specialty_link+"/"+state
    state_parameter=state
    coded_cities=get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(2)
        city_parameter=city
        city_link=state_link+"/"+city
        print(city)
        print(city_link)
        try:
            new_city_df=scrape_pages(city_link,macro_parameter, state_parameter, city_parameter)
            temp_master=pd.concat([temp_master,new_city_df])
        except:
            pass


# In[34]:


temp_master.shape[0]


# In[73]:


## combine urology datasets
urology_df=pd.concat([master_df,temp_master])


# In[74]:


## pickle results

urology_df.to_pickle("./urology_all.pkl")


# In[62]:


## reset index
urology_df=urology_df.reset_index()


# In[63]:


non_duplicated=~urology_df.names.duplicated()


# In[69]:


urology_df[non_duplicated].tail(50)


# In[58]:


## deduplicate
urology_df=urology_df[non_duplicated]


# In[80]:


temp_master=pd.DataFrame(columns=["state","city","macro_specialty","names","detail_specialty","links","addresses"])


# In[81]:


temp_master.head()


# In[82]:


## run code for next specialty - pulmonology
target_specialty=coded_specialties[1]
base_link="https://doctor.webmd.com/providers/specialty/"
specialty_link=base_link+"/"+target_specialty
macro_parameter=target_specialty
for state in tqdm(coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link=specialty_link+"/"+state
    state_parameter=state
    coded_cities=get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(2)
        city_parameter=city
        city_link=state_link+"/"+city
        print(city)
        print(city_link)
        try:
            new_city_df=scrape_pages(city_link,macro_parameter, state_parameter, city_parameter)
            temp_master=pd.concat([temp_master,new_city_df])
        except:
            pass


# In[86]:


temp_master.head()


# In[87]:


## pickle results

temp_master.to_pickle("./pulmonology_all.pkl")


# In[90]:


coded_specialties[2]


# In[91]:


temp_master=pd.DataFrame(columns=["state","city","macro_specialty","names","detail_specialty","links","addresses"])


# In[93]:


## run code for next specialty - special infections
target_specialty=coded_specialties[2]
base_link="https://doctor.webmd.com/providers/specialty/"
specialty_link=base_link+"/"+target_specialty
macro_parameter=target_specialty
for state in tqdm(coded_states, desc="States", leave=False):
    print("Checking state: " + state + "...")
    state_link=specialty_link+"/"+state
    state_parameter=state
    coded_cities=get_cities(state_link)
    for city in tqdm(coded_cities, desc="Cities", leave=False):
        time.sleep(1)
        city_parameter=city
        city_link=state_link+"/"+city
        print(city)
        print(city_link)
        try:
            new_city_df=scrape_pages(city_link,macro_parameter, state_parameter, city_parameter)
            temp_master=pd.concat([temp_master,new_city_df])
        except:
            pass


# In[96]:


## pickle results

temp_master.to_pickle("./special_all.pkl")


# In[98]:


## deduplicate

x=pd.read_pickle("./special_all.pkl")


# In[127]:


## clean urology

df=pd.read_pickle("./urology_all.pkl")
df.groupby("names")


# In[115]:


leland=x[x.names=="Dr. Leland Norcross Allen, MD"].head(10)


# In[125]:


leland.names.unique()


# In[102]:


x[x.names=="Dr. Leland Norcross Allen, MD"]


# In[6]:


## scrape through multiple pages
def scrape_pages(base_link, macro_parameter, state_parameter, city_parameter):
    page_number=1
    new_adds=True
    base_df=pd.DataFrame(columns=["state","city","macro_specialty","names","detail_specialty","links","addresses"])
    # do first page
    sample_link=base_link+"?pagenumber="+str(page_number)
    new_df=scrape_page(sample_link,macro_parameter, state_parameter, city_parameter)
    combined_df=pd.concat([base_df,new_df],axis=0)

    # loop, checking if first doc is the same
    while new_adds==True:
        page_number=page_number+1
        sample_link=base_link+"/"+"?pagenumber="+str(page_number)
        new_df=scrape_page(sample_link,macro_parameter, state_parameter, city_parameter)
#         time.sleep(1)
        print(new_df.names.iloc[0])
        if new_df.iloc[0].names==combined_df.iloc[0].names:
            print("No new names found, end of pages reached")
            new_adds=False
        else:
            combined_df=pd.concat([combined_df,new_df])
            print("Page " + str(page_number)+" scraped successfully.")
    return combined_df


# In[ ]:





# In[ ]:





# In[ ]:


master_df.shape[0]


# In[268]:


new_df.head()


# In[269]:


while new_adds==True:
    page_number=page_number+1
    sample_link=base_link+"/"+"?pagenumber="+str(page_number)
    new_df=scrape_page(sample_link)
    print(new_df.names.iloc[0])
    if new_df.iloc[0].names==combined_df.iloc[0].names:
        print("No new names found, end of pages reached")
        new_adds=False
    else:
        combined_df=pd.concat([combined_df,new_df])
        print("Page " + str(page_number)+" scraped successfully.")


# In[264]:





# In[ ]:


while new_adds==True:
    sample_link=sample_link+"/"+coded_cities[1]+"?pagenumber="+str(page_number)
    scrape_page
    


# In[7]:


## proceed with link scrape for single city for pulmonology
# get the full link for first page
def scrape_page(link, macro_parameter, state_parameter, city_parameter):
    # get page url
    response=requests.get(link)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # lists for dataframe
    names=[]
    detail_specialty=[]
    links=[]
    addresses=[]
    macro_specialty=[]
    states=[]
    cities=[]
    
    # Find all div elements with the class 'card-content'
    card_content_divs = soup.find_all('div', class_='card-content')
    
    # Iterate over each card
    for card_content_div in card_content_divs:
        # Extracting data from within the 'card-content' div
        if card_content_div is not None:
            ## individual card
            doc_name = card_content_div.find('h2').find('a').get_text(strip=True) ## how get name
            names.append(doc_name)
            doc_specialty = card_content_div.find('p', class_='prov-specialty').get_text(strip=True) ## how get detailed specialty
            detail_specialty.append(doc_specialty)
            doc_link=card_content_div.find('h2').find('a').get('href') ## how get individual doctor link
            links.append(doc_link)
            doc_address = card_content_div.find('address').find('span').get_text(strip=True)
            addresses.append(doc_address)
            states.append(state_parameter)
            cities.append(city_parameter)
            macro_specialty.append(macro_parameter)
        else:
            print("No data found for a card.")
    ## compile page data
    page_data={"state":states, "city":cities, "macro_specialty":macro_specialty,
          "names":names, "detail_specialty":detail_specialty,"links":links,"addresses":addresses}
    df=pd.DataFrame(page_data)
    return df


# In[207]:


print(sample_link)


# In[211]:


sample_df=scrape_page(sample_link)


# In[212]:


sample_df.tail()


# In[181]:


## proceed with link scrape for single city for pulmonology

response=requests.get("https://doctor.webmd.com/providers/specialty/urology/alabama/alexander-city?pagenumber=1")
soup = BeautifulSoup(response.content, 'html.parser')


# In[182]:


card_ = soup.find_all('div', class_='card-content')
len(div_elements)


# In[183]:


# Find all div elements with the class 'card-content'
card_content_divs = soup.find_all('div', class_='card-content')
len(card_content_divs)


# In[185]:


## individual card
card_content_div=card_content_divs[0]
doc_name = card_content_div.find('h2').find('a').get_text(strip=True) ## how get name
print(doc_name)
doc_specialty = card_content_div.find('p', class_='prov-specialty').get_text(strip=True) ## how get detailed specialty
print(doc_specialty)
doc_link=card_content_div.find('h2').find('a').get('href') ## how get individual doctor link
print(doc_link)
doc_address = card_content_div.find('address').find('span').get_text(strip=True)
print(doc_address)


# In[143]:


## lists for dataframe
names=[]
detail_specialty=[]
links=[]
addresses=[]
macro_specialty=[]
states=[]
cities=[]


# In[144]:


## scraping parameter inputs
state_parameter="Alabama"
city_parameter="Alabaster"
macro_parameter="Pulmonology"

## Iterate over each card
for card_content_div in card_content_divs:
    # Extracting data from within the 'card-content' div
    if card_content_div is not None:
        ## individual card
        doc_name = card_content_div.find('h2').find('a').get_text(strip=True) ## how get name
        names.append(doc_name)
        print(doc_name)
        doc_specialty = card_content_div.find('p', class_='prov-specialty').get_text(strip=True) ## how get detailed specialty
        detail_specialty.append(doc_specialty)
        print(doc_specialty)
        doc_link=card_content_div.find('h2').find('a').get('href') ## how get individual doctor link
        links.append(doc_link)
        print(doc_link)
        doc_address = card_content_div.find('address').find('span').get_text(strip=True)
        addresses.append(doc_address)
        print(doc_address)
        states.append(state_parameter)
        cities.append(city_parameter)
        macro_specialty.append(macro_parameter)
    else:
        print("No data found for a card.")


# In[146]:


## compile page data
data={"state":states, "city":cities, "macro_specialty":macro_specialty,
      "names":names, "detail_specialty":detail_specialty,"links":links,"addresses":addresses}
df=pd.DataFrame(data)
df.tail()


# In[74]:


df.to_csv("sample_doc_list.csv")


# In[4]:


get_ipython().system('pip install pandas --upgrade')


# In[ ]:




