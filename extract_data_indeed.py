import requests
from bs4 import BeautifulSoup
import pandas as pd
# Charger la page souhaitée
#url = 'https://fr.indeed.com/jobs?q=data+engineer&l=%C3%8Ele-de-France&start=0'
 


def extract(page):
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0'
    }

    proxy_params = {
        'api_key': '4aa4bc10-3dd0-432f-9ec6-75cb2e67caa6',
        'url': f'https://fr.indeed.com/jobs?q=data+ingenieur&l=France&radius=25&start={page}',
        }
    

 #   try:
    r = requests.get(
            url='https://proxy.scrapeops.io/v1/',
            params=proxy_params,
            headers=headers,
            timeout= 120
        )
        
    soup = BeautifulSoup(r.content, "html.parser")
    return soup    
   

   

def transform(soup):
    divs = soup.find_all('div', class_ = 'slider_container')    
 #  return len(divs) , 
    for item in divs:
        
        title = item.find('h2').text.strip() 
        
        location = item.find_next('div', class_='css-1restlb').text.strip()  # Vérifie l'élément qui suit
 
        company_name = item.find_next('span', class_ = 'css-1h7lukg ').text.strip() 
        try:
            salary = item.find('div', class_ = 'css-18z4q2i').text.strip()
        except:
            salary= ''
        summary = item.find('div',class_='jobMetaDataGroup').text.strip().replace('\n', '')

     
        job = {
                       
            "Origin" : "Indeed",
            "id": None,
            "title": title,
            "publication_date": None,
            "company": company_name,
            "location": location,
            "city": None,
            "code_postal": None,
            "salary": salary,
            "contract_type": None,
            "category": None,
            "description": summary
            
        }







        joblist.append(job)
    return

joblist = []     

for i in range (0, 80,10):
    print(f'Getting page,{i}')
    c = extract(0)
    transform(c)
#print(len(joblist))
df = pd.DataFrame(joblist)
print(df.head())
df.to_csv('jobs.csv')