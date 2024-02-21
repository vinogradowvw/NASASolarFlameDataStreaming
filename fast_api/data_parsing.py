import requests
from datetime import datetime


async def get_solar_data(url: str='https://api.nasa.gov/DONKI/FLR?api_key=sVjUtIbZu7gbMoVgec0GXIz1USUs5xikmF3uzWcD') -> dict:
    '''
    Requesting the data from the NASA api about Solar Flames
    '''
    response = requests.get(url)
    
    if response.status_code == 200:
        response = response.json()
        return response
    else:
        print('Error while requesting the Solar data:', response.status_code)
        print(response)
        return {}

    

async def get_notifications_data(url: str='https://api.nasa.gov/DONKI/notifications?type=FLR&api_key=sVjUtIbZu7gbMoVgec0GXIz1USUs5xikmF3uzWcD') -> dict:
    '''
    Requesting and parsing the notifications from the NASA api about Solar Flames
    '''
    response = requests.get(url)
    
    if response.status_code == 200:
        response = response.json()
        response = response[0]
        return response
    else:
        print('Error while requesting the Mars data:', response.status_code)
        print(response)
        return {}
