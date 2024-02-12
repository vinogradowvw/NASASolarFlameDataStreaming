import requests
from datetime import datetime


def request_parse_data(url: str='https://api.nasa.gov/DONKI/FLR?api_key=sVjUtIbZu7gbMoVgec0GXIz1USUs5xikmF3uzWcD') -> dict:
    '''
    Requesting and parsing the data from the NASA api 
    '''
    response = requests.get(url)
    
    if response.status_code == 200:
        response = response.json()
    else:
        print('Error:', response.status_code)
        print(response)
        return {}

    solar_data = {
        'duration' : [],
        'class_type_value' : [],
        'time_stamp' : []
    }

    def parse_class_type(classType: str) -> float:
        class_map = {
            'A' : 10,
            'B' : 20,
            'C' : 30,
            'M' : 40,
            'X' : 50
        }
        return class_map[classType[0]] + float(classType[1:])
        
    for solar_flare in response:
        solar_data['class_type_value'].append(parse_class_type(solar_flare['classType']))
        duration = datetime.strptime(solar_flare['endTime'], '%Y-%m-%dT%H:%MZ') - datetime.strptime(solar_flare['beginTime'], '%Y-%m-%dT%H:%MZ')
        duration = duration.seconds
        solar_data['duration'].append(
            duration
        )
        solar_data['time_stamp'].append(solar_flare['peakTime'])
    
    return solar_data

print(request_parse_data())
